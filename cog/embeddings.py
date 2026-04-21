import array
import math
import heapq
import logging
from math import isclose

from cog.core import Record
from cog.embedding_providers import EMBEDDING_PROVIDERS, _chunked

# Optional simsimd for SIMD-optimized similarity
try:
    import simsimd
    _HAS_SIMSIMD = True
except ImportError:
    _HAS_SIMSIMD = False

logger = logging.getLogger(__name__)


class EmbeddingMixin:
    """Mixin providing embedding/vector methods for Graph."""

    def put_embedding(self, text, embedding):
        """
        Saves a text embedding.
        """
        assert isinstance(text, str), "text must be a string"
        if self._cloud:
            self._cloud_client.mutate_put_embedding(text, embedding)
            return
        self.cog.use_namespace(self.graph_name).use_table(self.config.EMBEDDING_SET_TABLE_NAME).put(Record(
            text, embedding))

    def get_embedding(self, text):
        """
        Returns a text embedding.
        """
        assert isinstance(text, str), "text must be a string"
        if self._cloud:
            result = self._cloud_client.query_get_embedding(text)
            return result.get("embedding")
        record = self.cog.use_namespace(self.graph_name).use_table(self.config.EMBEDDING_SET_TABLE_NAME).get(
            text)
        if record is None:
            return None
        return record.value

    def delete_embedding(self, text):
        """
        Deletes a text embedding.
        """
        assert isinstance(text, str), "text must be a string"
        if self._cloud:
            self._cloud_client.mutate_delete_embedding(text)
            return
        self.cog.use_namespace(self.graph_name).use_table(self.config.EMBEDDING_SET_TABLE_NAME).delete(
            text)

    def put_embeddings_batch(self, text_embedding_pairs):
        """
        Bulk insert multiple embeddings efficiently.

        :param text_embedding_pairs: List of (text, embedding) tuples where text is an arbitrary string
        :return: self for method chaining

        Example:
            g.put_embeddings_batch([
                ("quick brown fox", [0.1, 0.2, ...]),
                ("jumps over the lazy dog", [0.3, 0.4, ...]),
            ])
        """
        if self._cloud:
            batch = [{"text": t, "embedding": e} for t, e in text_embedding_pairs]
            self._cloud_client.mutate_put_embeddings_batch(batch)
            return self
        self.cog.use_namespace(self.graph_name)
        self.cog.begin_batch()
        try:
            for text, embedding in text_embedding_pairs:
                if not isinstance(text, str):
                    raise TypeError("text must be a string")
                self.cog.use_table(self.config.EMBEDDING_SET_TABLE_NAME).put(Record(
                    text, embedding))
        finally:
            self.cog.end_batch()
        return self

    def scan_embeddings(self, limit=100):
        """
        Scan and return a list of texts that have embeddings stored.

        :param limit: Maximum number of embeddings to return
        :return: Dictionary with 'result' containing list of texts with embeddings

        Note: This scans the graph vertices and checks which have embeddings.
        """
        if self._cloud:
            result = self._cloud_client.query_scan_embeddings(limit)
            result.pop("ok", None)
            return result
        result = []
        self.cog.use_namespace(self.graph_name).use_table(self.config.GRAPH_NODE_SET_TABLE_NAME)
        count = 0
        for r in self.cog.scanner():
            if count >= limit:
                break
            text = r.key
            if self.get_embedding(text) is not None:
                result.append({"id": text})
                count += 1
        return {"result": result}

    def embedding_stats(self):
        """
        Return statistics about stored embeddings.
        
        :return: Dictionary with count and dimensions (if available)
        """
        if self._cloud:
            result = self._cloud_client.query_embedding_stats()
            result.pop("ok", None)
            return result
        count = 0
        dimensions = None
        # Scan the embedding table directly
        self.cog.use_namespace(self.graph_name).use_table(self.config.EMBEDDING_SET_TABLE_NAME)
        for r in self.cog.scanner():
            count += 1
            if dimensions is None and r.value is not None:
                dimensions = len(r.value)
        return {"count": count, "dimensions": dimensions}

    def k_nearest(self, text, k=10):
        """
        Find the k vertices most similar to the given text based on embeddings.

        :param text: The string to find similar vertices for (can be a word, phrase, or sentence)
        :param k: Number of nearest neighbors to return (default 10)
        :return: self for method chaining

        Example:
            g.v().k_nearest("machine learning is transforming industries", k=5).all()
        """
        if self._cloud:
            return self._cloud_append("k_nearest", text=text, k=k)
        # Auto-embed query text if missing
        self._auto_embed(text)

        target_embedding = self.get_embedding(text)
        if target_embedding is None:
            self.last_visited_vertices = []
            return self
        
        # simsimd/fallback requires buffer protocol (e.g. numpy array or python array)
        target_vec = array.array('f', target_embedding)
        similarities = []
        
        # None = no prior traversal, scan entire embedding table
        # [] = prior traversal returned empty, preserve empty semantics
        # [...] = search within visited vertices
        if self.last_visited_vertices is None:
            # Scan embedding table directly for all embeddings
            self.cog.use_namespace(self.graph_name).use_table(self.config.EMBEDDING_SET_TABLE_NAME)
            for r in self.cog.scanner():
                if r.value is not None:
                    v_vec = array.array('f', r.value)
                    distance = self._cosine_distance(target_vec, v_vec)
                    similarity = 1.0 - float(distance)
                    from cog.torque import Vertex
                    similarities.append((similarity, Vertex(r.key)))
        elif self.last_visited_vertices:
            # Search within visited vertices
            for v in self.last_visited_vertices:
                v_embedding = self.get_embedding(v.id)
                if v_embedding is not None:
                    v_vec = array.array('f', v_embedding)
                    distance = self._cosine_distance(target_vec, v_vec)
                    similarity = 1.0 - float(distance)
                    similarities.append((similarity, v))
        # else: empty list, similarities stays empty
        
        # Get top k using heap for efficiency
        top_k = heapq.nlargest(k, similarities, key=lambda x: x[0])
        self.last_visited_vertices = [v for _, v in top_k]
        return self

    def sim(self, text, operator, threshold, strict=False):
        """
            Applies cosine similarity filter to the vertices and removes any vertices that do not pass the filter.

            Parameters:
            -----------
            text: str
                The string (word, phrase, or sentence) to compare to the other vertices.
            operator: str
                The comparison operator to use. One of "==", ">", "<", ">=", "<=", or "in".
            threshold: float or list of 2 floats
                The threshold value(s) to use for the comparison. If operator is "==", ">", "<", ">=", or "<=", threshold should be a float. If operator is "in", threshold should be a list of 2 floats.
            strict: bool, optional
                If True, raises an exception if an embedding is not found for either text. If False, assigns a similarity of 0.0 to any text whose embedding is not found.

            Returns:
            --------
            self: GraphTraversal
                Returns self to allow for method chaining.

            Raises:
            -------
            ValueError:
                If operator is not a valid comparison operator or if threshold is not a valid threshold value for the given operator.
                If strict is True and an embedding is not found for either text.
    """
        if self._cloud:
            return self._cloud_append("sim", text=text, operator=operator,
                                      threshold=threshold, strict=strict)
        if not isinstance(threshold, (float, int, list)):
            raise ValueError("Invalid threshold value: {}".format(threshold))

        if operator == 'in':
            if not isinstance(threshold, list) or len(threshold) != 2:
                raise ValueError("Invalid threshold value: {}".format(threshold))
            if not all(isinstance(t, (float, int)) for t in threshold):
                raise ValueError("Invalid threshold value: {}".format(threshold))

        # Auto-embed query text if missing
        self._auto_embed(text)

        filtered_vertices = []
        for v in self.last_visited_vertices:
            similarity = self._cosine_similarity(text, v.id)
            if not similarity:
                # similarity is None if an embedding is not found for either text.
                if strict:
                    raise ValueError("Missing embedding for either '{}' or '{}'".format(text, v.id))
                else:
                    # Treat vertices without embeddings as if they have no similarity to any other text.
                    similarity = 0.0
            if operator == '=':
                if isclose(similarity, threshold):
                    filtered_vertices.append(v)
            elif operator == '>':
                if similarity > threshold:
                    filtered_vertices.append(v)
            elif operator == '<':
                if similarity < threshold:
                    filtered_vertices.append(v)
            elif operator == '>=':
                if similarity >= threshold:
                    filtered_vertices.append(v)
            elif operator == '<=':
                if similarity <= threshold:
                    filtered_vertices.append(v)
            elif operator == 'in':
                if not threshold[0] <= similarity <= threshold[1]:
                    continue
                filtered_vertices.append(v)
            else:
                raise ValueError("Invalid operator: {}".format(operator))
        self.last_visited_vertices = filtered_vertices
        return self

    def _cosine_distance(self, x, y):
        """Compute cosine distance (1 - similarity) with simsimd or pure Python fallback."""
        if _HAS_SIMSIMD:
            return simsimd.cosine(x, y)
        else:
            # Pure Python fallback for Pyodide/environments without simsimd
            dot = sum(a * b for a, b in zip(x, y))
            norm_x = math.sqrt(sum(a * a for a in x))
            norm_y = math.sqrt(sum(b * b for b in y))
            if norm_x == 0 or norm_y == 0:
                return 1.0  # Max distance if either vector is zero
            return 1.0 - (dot / (norm_x * norm_y))

    def _cosine_similarity(self, text1, text2):
        """Compute cosine similarity between two texts using SIMD-optimized simsimd library or pure Python fallback."""
        x_list = self.get_embedding(text1)
        y_list = self.get_embedding(text2)

        if x_list is None or y_list is None:
            return None
        
        # Use python array for buffer protocol (compatible with simsimd)
        x = array.array('f', x_list)
        y = array.array('f', y_list)

        # cosine distance = 1 - similarity, so we convert
        distance = self._cosine_distance(x, y)
        return 1.0 - float(distance)

    def load_glove(self, filepath, limit=None, batch_size=1000):
        """
        Load GloVe embeddings from a text file.
        
        :param filepath: Path to GloVe file (e.g., 'glove.6B.100d.txt')
        :param limit: Maximum number of embeddings to load (None for all)
        :param batch_size: Number of embeddings to batch before writing (default 1000)
        :return: Number of embeddings loaded
        
        Example:
            count = g.load_glove("glove.6B.100d.txt", limit=50000)
        """
        count = 0
        batch = []
        
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                if limit is not None and count >= limit:
                    break
                parts = line.strip().split()
                if len(parts) < 2:
                    continue
                word = parts[0]
                embedding = [float(x) for x in parts[1:]]
                batch.append((word, embedding))
                count += 1
                
                if len(batch) >= batch_size:
                    self.put_embeddings_batch(batch)
                    batch = []
        
        # Load remaining batch
        if batch:
            self.put_embeddings_batch(batch)
        
        return count

    def load_gensim(self, model, limit=None, batch_size=1000):
        """
        Load embeddings from a Gensim Word2Vec or FastText model.
        
        :param model: A Gensim model with a 'wv' attribute (Word2Vec, FastText)
        :param limit: Maximum number of embeddings to load (None for all)
        :param batch_size: Number of embeddings to batch before writing (default 1000)
        :return: Number of embeddings loaded
        
        Example:
            from gensim.models import Word2Vec
            model = Word2Vec(sentences)
            count = g.load_gensim(model)
        """
        count = 0
        batch = []
        
        # Get word vectors from model
        if hasattr(model, 'wv'):
            wv = model.wv
        else:
            wv = model  # Already a KeyedVectors object
        
        for word in wv.index_to_key:
            if limit is not None and count >= limit:
                break
            embedding = wv[word].tolist()
            batch.append((word, embedding))
            count += 1
            
            if len(batch) >= batch_size:
                self.put_embeddings_batch(batch)
                batch = []
        
        if batch:
            self.put_embeddings_batch(batch)
        
        return count

    def _auto_embed(self, text):
        """Auto-fetch and store embedding for a text string if missing.
        Only active after vectorize() has been explicitly called."""
        if not self._vectorize_configured:
            return
        if self.get_embedding(text) is not None:
            return
        try:
            provider_fn = EMBEDDING_PROVIDERS[self._default_provider]
            pairs = provider_fn([text], **self._default_provider_kwargs)
            if pairs:
                self.put_embeddings_batch(pairs)
        except Exception as e:
            self.logger.debug("auto-embed for '{}' failed: {}".format(text, e))

    def vectorize(self, texts=None, provider="cogdb", batch_size=100, **kwargs):
        """
        Auto-generate vector embeddings using a provider.
        Skips texts that already have embeddings.

        Providers:
            "cogdb" (default): CogDB's hosted vector service. Input strings are
                sent over HTTPS to the CogDB vector endpoint and processed at the
                edge node closest to your geographic location. Processing is
                transient. Inputs and outputs are not stored. Only
                embedding vectors are returned. Use provider="custom" to route
                through a self-hosted or alternative endpoint.
            "openai": Calls the OpenAI embeddings API directly. Requires api_key=.
                Data is sent to OpenAI and subject to OpenAI's data handling policies.
            "custom": Route requests to a user-provided endpoint. Requires url=.
                Useful for self-hosted inference or air-gapped environments.

        :param texts: Optional — a string or list of strings to embed.
                      If None, embeds all nodes in the graph.
        :param provider: Provider name — "cogdb" (default), "openai", or "custom".
        :param batch_size: Number of texts per provider request (default 100).
        :param kwargs: Passed to the provider (e.g. url=, api_key=, model=).
        :return: Summary dict {"vectorized": N, "skipped": M, "total": T}

        Example:
            g.vectorize()                                        # all nodes
            g.vectorize("europa")                                # single string
            g.vectorize(["europa", "ocean floor mapping"])       # specific strings
            g.vectorize(provider="openai", api_key="sk-...")
        """
        if self._cloud:
            t = texts
            if isinstance(t, str):
                t = [t]
            return self._cloud_client.mutate_vectorize(t, provider, batch_size)
        if not isinstance(batch_size, int) or batch_size < 1:
            raise ValueError("batch_size must be a positive integer, got: {}".format(batch_size))

        if provider not in EMBEDDING_PROVIDERS:
            raise ValueError("Unknown provider '{}'. Choose from: {}".format(
                provider, ", ".join(EMBEDDING_PROVIDERS.keys())))

        # Store provider config for auto-embed in queries
        self._default_provider = provider
        self._default_provider_kwargs = kwargs
        self._vectorize_configured = True

        provider_fn = EMBEDDING_PROVIDERS[provider]

        if texts is not None:
            if isinstance(texts, str):
                texts = [texts]
            all_texts = texts
        else:
            all_texts = []
            self.cog.use_namespace(self.graph_name).use_table(self.config.GRAPH_NODE_SET_TABLE_NAME)
            for r in self.cog.scanner():
                all_texts.append(r.key)

        total = len(all_texts)

        # Skip texts that already have embeddings
        to_embed = [t for t in all_texts if self.get_embedding(t) is None]
        skipped = total - len(to_embed)

        if not to_embed:
            return {"vectorized": 0, "skipped": skipped, "total": total}

        # Send to provider in batches and store results
        vectorized = 0
        errors = []
        for chunk in _chunked(to_embed, batch_size):
            try:
                pairs = provider_fn(chunk, **kwargs)
                self.put_embeddings_batch(pairs)
                vectorized += len(pairs)
            except Exception as e:
                self.logger.error("vectorize batch failed: {}".format(e))
                errors.append(str(e))

        result = {"vectorized": vectorized, "skipped": skipped, "total": total}
        if errors:
            result["errors"] = errors
        return result
