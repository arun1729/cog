"""
Export utilities for CogDB graphs.

Provides functions to extract triples from a graph and export them
to files in various formats (N-Triples, CSV, TSV).
"""
import csv as csv_module
from cog.database import out_nodes


def _is_iri(term):
    """Check if a term is already wrapped in angle brackets (IRI)."""
    return term.startswith("<") and term.endswith(">")


def _is_blank_node(term):
    """Check if a term is a blank node (_:label)."""
    return term.startswith("_:")


def _to_nt_term(term, position="object"):
    """
    Format a term for W3C N-Triples strict output.

    - If already an IRI (<...>), pass through.
    - If a blank node (_:...), pass through.
    - Subject/predicate that are plain strings get wrapped in <...>.
    - Object plain strings become quoted literals "...".
    """
    if _is_iri(term) or _is_blank_node(term):
        return term
    if position in ("subject", "predicate"):
        # Escape characters that would break IRI syntax
        escaped = term.replace("\\", "\\\\").replace(">", "%3E")
        return "<{}>".format(escaped)
    else:
        # Escape backslashes, quotes, newlines, carriage returns per spec
        escaped = (term
                   .replace("\\", "\\\\")
                   .replace('"', '\\"')
                   .replace("\n", "\\n")
                   .replace("\r", "\\r"))
        return '"{}"'.format(escaped)


def get_triples(graph):
    """
    Extract all triples from a graph as a generator.

    Iterates over every vertex and every predicate to reconstruct
    the complete set of triples stored in the graph.

    :param graph: A Graph instance.
    :return: A generator of (subject, predicate, object) tuples.

    Example:
        from cog.export import get_triples
        for s, p, o in get_triples(g):
            print(s, p, o)
    """
    graph.cog.use_namespace(graph.graph_name)
    # Scan all vertices
    graph.cog.use_table(graph.config.GRAPH_NODE_SET_TABLE_NAME)
    vertices = [r.key for r in graph.cog.scanner()]

    # Collect valid predicate hashes (skip internal tables)
    internal = (graph.config.GRAPH_NODE_SET_TABLE_NAME, graph.config.GRAPH_EDGE_SET_TABLE_NAME)
    predicates = [
        (ph, graph._predicate_reverse_lookup_cache.get(ph, ph))
        for ph in graph.all_predicates if ph not in internal
    ]

    # For each predicate, check outgoing edges from each vertex
    for pred_hash, predicate_name in predicates:
        for vertex in vertices:
            record = graph.cog.use_table(pred_hash).get(out_nodes(vertex))
            if record is not None:
                if record.value_type == "s":
                    yield (vertex, predicate_name, str(record.value))
                elif record.value_type in ("l", "u"):
                    for obj in record.value:
                        yield (vertex, predicate_name, obj)


def export_triples(graph, filepath, fmt="nt", strict=False):
    """
    Export all triples in the graph to a file.

    Writes one triple per line in the specified format.

    :param graph: A Graph instance.
    :param filepath: Path to the output file.
    :param fmt: Format string — "nt" (N-Triples, default), "csv", or "tsv".
    :param strict: If True and fmt is "nt", output W3C-compliant N-Triples
                   where IRIs are wrapped in <>, blank nodes use _: prefix,
                   and plain literals are quoted with "".
                   See https://www.w3.org/TR/n-triples/
    :return: Number of triples written.

    Example:
        g.export("graph.nt")                        # N-Triples (default)
        g.export("graph.nt", strict=True)            # W3C strict N-Triples
        g.export("graph.csv", fmt="csv")             # CSV with header
        g.export("graph.tsv", fmt="tsv")             # TSV with header
    """
    fmt = fmt.lower()
    count = 0

    with open(filepath, 'w', newline='') as f:
        if fmt in ("csv", "tsv"):
            delimiter = '\t' if fmt == "tsv" else ','
            writer = csv_module.writer(f, delimiter=delimiter)
            writer.writerow(["subject", "predicate", "object"])
            for s, p, o in get_triples(graph):
                writer.writerow([s, p, o])
                count += 1
        elif fmt == "nt":
            if strict:
                for s, p, o in get_triples(graph):
                    f.write('{} {} {} .\n'.format(
                        _to_nt_term(s, "subject"),
                        _to_nt_term(p, "predicate"),
                        _to_nt_term(o, "object"),
                    ))
                    count += 1
            else:
                for s, p, o in get_triples(graph):
                    f.write('{} {} {} .\n'.format(s, p, o))
                    count += 1
        else:
            raise ValueError("Unsupported format '{}'. Use 'nt', 'csv', or 'tsv'.".format(fmt))

    return count
