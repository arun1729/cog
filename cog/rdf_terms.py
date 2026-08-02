"""
Term codec: the mapping between rdflib terms and CogDB stored strings.

CogDB stores vertex ids and predicate names as plain strings. RDF terms are
richer — an IRI, a blank node, or a literal carrying an optional language tag
or datatype. This module defines the single, bidirectional rule for moving
between the two so that distinct RDF terms are always distinct stored strings
and every stored string decodes back to exactly one term.

Encoding rules (N-Triples syntax for literals and blank nodes, bare for IRIs):

    URIRef("alice")                     <->  alice
    URIRef("http://ex/alice")           <->  http://ex/alice
    Literal("chat")                     <->  "chat"            (quotes kept)
    Literal("chat", lang="fr")          <->  "chat"@fr
    Literal("42", datatype=xsd:integer) <->  "42"^^<http://www.w3.org/2001/XMLSchema#integer>
    BNode("b0")                         <->  _:b0

IRIs are stored bare (no angle brackets) so the two query surfaces see one
graph: a triple written with the Torque API — g.put("alice", "follows",
"bob") — is queryable from SPARQL as { <alice> <follows> ?x }, and an IRI
written through rdflib is addressable from Torque as g.v("http://ex/alice").
Vertex ids of existing graphs therefore decode as IRIs. Blank-node ids
created by put_json() already use the _:label convention and decode as
blank nodes.

RFC 3987 forbids the characters '"', "'" and the "_:" prefix shape in IRIs,
so real IRIs never collide with the literal/blank-node encodings. A URIRef
that nevertheless starts with one of these (only constructible by hand) is
stored as-is and will not round-trip; everything else is bijective.
"""
from functools import lru_cache

from rdflib import BNode, Literal, URIRef
from rdflib.util import from_n3


def encode_term(term):
    """Encode an rdflib term to its CogDB stored-string form."""
    # Order matters: URIRef, Literal and BNode are all str subclasses.
    if isinstance(term, URIRef):
        return str(term)
    if isinstance(term, (Literal, BNode)):
        return term.n3()
    raise TypeError(
        "expected an rdflib URIRef, Literal or BNode, got {!r}".format(term))


@lru_cache(maxsize=65536)
def decode_term(stored):
    """Decode a CogDB stored string back to an rdflib term."""
    if stored.startswith(('"', "'")):
        try:
            term = from_n3(stored)
        except Exception:
            term = None
        # from_n3 is lenient and can misparse strings that are not valid
        # N-Triples syntax. Accept the parse only if it reproduces the
        # stored string exactly; otherwise this is a legacy vertex id that
        # merely starts with a quote character, kept as a bare id.
        if isinstance(term, Literal) and term.n3() == stored:
            return term
        return URIRef(stored)
    if stored.startswith("_:"):
        return BNode(stored[2:])
    return URIRef(stored)
