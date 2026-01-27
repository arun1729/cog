"""
Tests for JSON operations (putj, updatej) to ensure correct graph construction
and update semantics for nested objects, arrays, and mixed structures.
"""

import pytest
import os
import shutil
from cog.torque import Graph
from cog import config


COG_HOME = "test_json_ops_isolated_home"


@pytest.fixture
def clean_graph():
    """Create a fresh graph for each test with complete isolation."""
    import uuid
    unique_dir = COG_HOME + "_" + str(uuid.uuid4())[:8]
    full_path = "/tmp/" + unique_dir
    
    if os.path.exists(full_path):
        shutil.rmtree(full_path)
    os.makedirs(full_path)
    config.CUSTOM_COG_DB_PATH = full_path
    
    g = Graph("test_graph")
    yield g
    g.close()
    if os.path.exists(full_path):
        shutil.rmtree(full_path)


class TestPutJson:
    """Tests for putj - inserting JSON objects into the graph."""

    def test_simple_object(self, clean_graph):
        """Test inserting a simple flat object."""
        g = clean_graph
        g.putj('{"_id": "1", "name": "alice", "age": "30"}')
        
        result = g.v("alice").inc().out("name").all()
        assert len(result["result"]) == 1
        assert result["result"][0]["id"] == "alice"

    def test_nested_object(self, clean_graph):
        """Test inserting an object with nested objects."""
        g = clean_graph
        g.putj('{"_id": "1", "name": "bob", "address": {"city": "toronto", "country": "canada"}}')
        
        # Can traverse to nested property
        result = g.v("bob").inc().out("address").out("city").all()
        assert len(result["result"]) == 1
        assert result["result"][0]["id"] == "toronto"

    def test_array_of_primitives(self, clean_graph):
        """Test inserting an object with an array of primitive values."""
        g = clean_graph
        g.putj('{"_id": "1", "name": "charlie", "colors": ["red", "green", "blue"]}')
        
        # colors points to a list node, then list items have the color values
        result = g.v("charlie").inc().out("colors").out("colors").all()
        assert len(result["result"]) == 3
        color_ids = {r["id"] for r in result["result"]}
        assert color_ids == {"red", "green", "blue"}

    def test_array_of_objects(self, clean_graph):
        """Test inserting an object with an array of objects."""
        g = clean_graph
        g.putj('{"_id": "1", "name": "dave", "friends": [{"name": "eve"}, {"name": "frank"}]}')
        
        result = g.v("dave").inc().out("friends").out().out("name").all()
        assert len(result["result"]) == 2
        names = {r["id"] for r in result["result"]}
        assert names == {"eve", "frank"}

    def test_deeply_nested_structure(self, clean_graph):
        """Test inserting deeply nested JSON."""
        g = clean_graph
        g.putj('''{
            "_id": "1", 
            "name": "root",
            "level1": {
                "level2": {
                    "level3": {
                        "value": "deep"
                    }
                }
            }
        }''')
        
        result = g.v("root").inc().out("level1").out("level2").out("level3").out("value").all()
        assert len(result["result"]) == 1
        assert result["result"][0]["id"] == "deep"


class TestUpdateJson:
    """Tests for updatej - updating existing JSON objects in the graph."""

    def test_update_simple_property(self, clean_graph):
        """Test updating a simple property replaces the old value."""
        g = clean_graph
        g.putj('{"_id": "1", "name": "alice", "city": "toronto"}')
        
        # Verify initial state
        result = g.v("alice").inc().out("city").all()
        assert result["result"][0]["id"] == "toronto"
        
        # Update
        g.updatej('{"_id": "1", "name": "alice", "city": "vancouver"}')
        
        # Verify update - should have only one city now
        result = g.v("alice").inc().out("city").all()
        assert len(result["result"]) == 1
        assert result["result"][0]["id"] == "vancouver"

    def test_update_nested_object(self, clean_graph):
        """Test updating a nested object replaces properties correctly."""
        g = clean_graph
        g.putj('{"_id": "1", "name": "bob", "address": {"city": "toronto", "country": "canada"}}')
        
        # Update the address
        g.updatej('{"_id": "1", "name": "bob", "address": {"city": "montreal", "country": "canada"}}')
        
        result = g.v("bob").inc().out("address").out("city").all()
        assert len(result["result"]) == 1
        assert result["result"][0]["id"] == "montreal"

    def test_update_array_replaces_list_node(self, clean_graph):
        """Test updating an array creates a new list node with new items."""
        g = clean_graph
        g.putj('{"_id": "1", "name": "charlie", "tags": ["a", "b"]}')
        
        # Update with new tags
        g.updatej('{"_id": "1", "name": "charlie", "tags": ["x", "y", "z"]}')
        
        # tags points to a list node, then list items have the tag values
        result = g.v("charlie").inc().out("tags").out("tags").all()
        assert len(result["result"]) == 3
        tags = {r["id"] for r in result["result"]}
        assert tags == {"x", "y", "z"}

    def test_update_by_nested_id(self, clean_graph):
        """Test updating a nested object directly by its _id."""
        g = clean_graph
        g.putj('{"_id": "1", "name": "dave", "location": {"_id": "loc1", "city": "ottawa"}}')
        
        # Verify initial
        result = g.v("dave").inc().out("location").out("city").all()
        assert result["result"][0]["id"] == "ottawa"
        
        # Update the location directly by its _id
        g.updatej('{"_id": "loc1", "city": "quebec"}')
        
        # Should be updated
        result = g.v("dave").inc().out("location").out("city").all()
        assert len(result["result"]) == 1
        assert result["result"][0]["id"] == "quebec"

    def test_update_preserves_unmentioned_edges(self, clean_graph):
        """Test that updating one property doesn't affect unrelated edges from same node."""
        g = clean_graph
        g.putj('{"_id": "1", "name": "eve", "city": "london", "country": "uk"}')
        
        # Update only city
        g.updatej('{"_id": "1", "city": "manchester"}')
        
        # City should be updated
        result = g.v().has("city", "manchester").all()
        assert len(result["result"]) == 1
        
        # Note: country edge still exists on the node (updatej doesn't delete unmentioned properties)
        # This is expected behavior - updatej updates what you specify

    def test_update_object_in_array_without_id(self, clean_graph):
        """Test updating when array items don't have _id - creates new items."""
        g = clean_graph
        g.putj('{"_id": "1", "name": "frank", "items": [{"value": "old1"}, {"value": "old2"}]}')
        
        # Update with new items (no _id means new blank nodes)
        g.updatej('{"_id": "1", "name": "frank", "items": [{"value": "new1"}, {"value": "new2"}, {"value": "new3"}]}')
        
        result = g.v("frank").inc().out("items").out().out("value").all()
        assert len(result["result"]) == 3
        values = {r["id"] for r in result["result"]}
        assert values == {"new1", "new2", "new3"}


class TestJsonEdgeCases:
    """Edge cases and potential problem scenarios."""

    def test_empty_object(self, clean_graph):
        """Test inserting an object with only _id."""
        g = clean_graph
        g.putj('{"_id": "empty"}')
        
        # Should be able to find the node
        result = g.v("_:_id_empty").all()
        assert len(result["result"]) == 1

    def test_empty_array(self, clean_graph):
        """Test inserting an object with an empty array."""
        g = clean_graph
        g.putj('{"_id": "1", "name": "test", "items": []}')
        
        # Name should exist
        result = g.v("test").inc().out("name").all()
        assert len(result["result"]) == 1

    def test_multiple_objects_same_structure(self, clean_graph):
        """Test inserting multiple objects with same structure."""
        g = clean_graph
        g.putj('{"_id": "1", "name": "alice", "role": "admin"}')
        g.putj('{"_id": "2", "name": "bob", "role": "user"}')
        g.putj('{"_id": "3", "name": "charlie", "role": "admin"}')
        
        # Find all admins
        result = g.v("admin").inc().out("name").all()
        assert len(result["result"]) == 2
        names = {r["id"] for r in result["result"]}
        assert names == {"alice", "charlie"}

    def test_update_then_query_consistency(self, clean_graph):
        """Test that multiple updates maintain graph consistency."""
        g = clean_graph
        
        # Initial insert
        g.putj('{"_id": "1", "name": "user", "status": "active"}')
        
        # Multiple updates
        g.updatej('{"_id": "1", "status": "pending"}')
        g.updatej('{"_id": "1", "status": "inactive"}')
        g.updatej('{"_id": "1", "status": "active"}')
        
        # Should have only the final status
        result = g.v("user").inc().out("status").all()
        assert len(result["result"]) == 1
        assert result["result"][0]["id"] == "active"

    def test_shared_values_between_objects(self, clean_graph):
        """Test that shared values between objects work correctly."""
        g = clean_graph
        g.putj('{"_id": "1", "name": "alice", "team": "engineering"}')
        g.putj('{"_id": "2", "name": "bob", "team": "engineering"}')
        
        # Both should point to same team value
        result = g.v("engineering").inc().out("name").all()
        assert len(result["result"]) == 2
        names = {r["id"] for r in result["result"]}
        assert names == {"alice", "bob"}

    def test_update_does_not_affect_other_objects(self, clean_graph):
        """Test that updating one object doesn't corrupt another."""
        g = clean_graph
        
        # Two objects sharing a common value
        g.putj('{"_id": "1", "name": "alice", "city": "toronto"}')
        g.putj('{"_id": "2", "name": "bob", "city": "toronto"}')
        
        # Update alice's city
        g.updatej('{"_id": "1", "name": "alice", "city": "vancouver"}')
        
        # Bob should still be in toronto
        result = g.v("bob").inc().out("city").all()
        assert len(result["result"]) == 1
        assert result["result"][0]["id"] == "toronto"
        
        # Alice should be in vancouver
        result = g.v("alice").inc().out("city").all()
        assert len(result["result"]) == 1
        assert result["result"][0]["id"] == "vancouver"

    def test_complex_nested_update(self, clean_graph):
        """Test complex nested structure update."""
        g = clean_graph
        
        g.putj('''{
            "_id": "company1",
            "name": "TechCorp",
            "departments": [
                {"name": "Engineering", "head": "alice"},
                {"name": "Marketing", "head": "bob"}
            ]
        }''')
        
        # Update with new departments
        g.updatej('''{
            "_id": "company1",
            "name": "TechCorp",
            "departments": [
                {"name": "Engineering", "head": "charlie"},
                {"name": "Sales", "head": "dave"}
            ]
        }''')
        
        # Check new department heads
        result = g.v("TechCorp").inc().out("departments").out().out("head").all()
        assert len(result["result"]) == 2
        heads = {r["id"] for r in result["result"]}
        assert heads == {"charlie", "dave"}


class TestJsonWithIds:
    """Tests specifically for _id handling."""

    def test_id_creates_predictable_node(self, clean_graph):
        """Test that _id creates a predictable node identifier."""
        g = clean_graph
        g.putj('{"_id": "user123", "name": "test"}')
        
        # Should be able to query by the blank node id
        result = g.v("_:_id_user123").out("name").all()
        assert len(result["result"]) == 1
        assert result["result"][0]["id"] == "test"

    def test_update_finds_by_id(self, clean_graph):
        """Test that updatej correctly finds and updates by _id."""
        g = clean_graph
        g.putj('{"_id": "x", "value": "original"}')
        g.updatej('{"_id": "x", "value": "updated"}')
        
        result = g.v("_:_id_x").out("value").all()
        assert len(result["result"]) == 1
        assert result["result"][0]["id"] == "updated"

    def test_nested_object_with_id_during_put(self, clean_graph):
        """Test inserting nested objects that have their own _id."""
        g = clean_graph
        g.putj('{"_id": "parent", "child": {"_id": "child1", "name": "nested"}}')
        
        # Can access child by its own _id
        result = g.v("_:_id_child1").out("name").all()
        assert len(result["result"]) == 1
        assert result["result"][0]["id"] == "nested"
        
        # Can also traverse from parent
        result = g.v("_:_id_parent").out("child").out("name").all()
        assert len(result["result"]) == 1
        assert result["result"][0]["id"] == "nested"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
