package trie_test

import (
	"encoding/json"
	"fmt"
	"github.com/workspace-9/gomq/types/pub/trie"
	"testing"
)

func TestTrie(t *testing.T) {
	trie := &trie.Trie[string]{}
	trie.Put("", "a")
	if err := ExpectStructure(`""`, trie); err != nil {
		t.Errorf(err.Error())
	}

	trie.Put("foo", "b")
	if err := ExpectStructure(`{"": "foo"}`, trie); err != nil {
		t.Errorf(err.Error())
	}

	trie.Put("foobar", "c")
	if err := ExpectStructure(`{"": {"foo": "foobar"}}`, trie); err != nil {
		t.Errorf(err.Error())
	}

	trie.Put("bazfoo", "d")
	if err := ExpectStructure(`{"": {"foo": "foobar", "bazfoo": 0}}`, trie); err != nil {
		t.Errorf(err.Error())
	}

	trie.Put("baz", "e")
	if err := ExpectStructure(`{"": {"foo": "foobar", "baz": "bazfoo"}}`, trie); err != nil {
		t.Errorf(err.Error())
	}

	expect := ExpectToVisit("a")
	trie.Query("", expect.Visit)
	if err := expect.Error(); err != nil {
		t.Errorf(err.Error())
	}

	expect = ExpectToVisit("a", "b")
	trie.Query("foo", expect.Visit)
	if err := expect.Error(); err != nil {
		t.Errorf(err.Error())
	}

	expect = ExpectToVisit("a", "e")
	trie.Query("baz", expect.Visit)
	if err := expect.Error(); err != nil {
		t.Errorf(err.Error())
	}

	trie.Remove("foo", "b")
	if err := ExpectStructure(`{"": {"foobar": 0, "baz": "bazfoo"}}`, trie); err != nil {
		t.Errorf(err.Error())
	}

	expect = ExpectToVisit("a")
	trie.Query("foo", expect.Visit)
	if err := expect.Error(); err != nil {
		t.Errorf(err.Error())
	}

	trie.Remove("", "a")
	if err := ExpectStructure(`{"": {"foobar": 0, "baz": "bazfoo"}}`, trie); err != nil {
		t.Errorf(err.Error())
	}

	trie.Remove("foobar", "c")
	if err := ExpectStructure(`{"": {"baz": "bazfoo"}}`, trie); err != nil {
		t.Errorf(err.Error())
	}

	trie.Remove("baz", "e")
	if err := ExpectStructure(`{"": "bazfoo"}`, trie); err != nil {
		t.Errorf(err.Error())
	}

	trie.Remove("bazfoo", "d")
	if err := ExpectStructure(`""`, trie); err != nil {
		t.Errorf(err.Error())
	}
}

func ExpectStructure(s string, trie *trie.Trie[string]) error {
	var data any
	if err := json.Unmarshal([]byte(s), &data); err != nil {
		panic(err)
	}

	return expectPath(data, "", trie)
}

func expectPath(anyData any, path string, trie *trie.Trie[string]) error {
	switch data := anyData.(type) {
	case float64:
		return fmt.Errorf("Expected nothing at path %s, found %s", path, trie.Key)
	case string:
		if data != trie.Key {
			return fmt.Errorf("Expected to find key %s at path %s, found %s", data, path, trie.Key)
		}
		return nil
	case map[string]any:
		if val, ok := data[trie.Key]; !ok {
			return fmt.Errorf("Couldn't find key %s at path %s", trie.Key, path)
		} else {
			for _, child := range trie.Children {
				if err := expectPath(val, path+"::"+trie.Key, child); err != nil {
					return err
				}
			}

			switch v := val.(type) {
			case map[string]any:
				if len(v) != len(trie.Children) {
					return fmt.Errorf("Expected %d children at path %s, got %d", len(v), path, len(trie.Children))
				}
				return nil
			case string:
				if len(trie.Children) != 1 {
					return fmt.Errorf("Expected 1 child at path %s, got %d", path, len(trie.Children))
				}
				return nil
			case float64:
				if len(trie.Children) != 0 {
					return fmt.Errorf("Expected 0 children at path %s, got %d", path, len(trie.Children))
				}
				return nil
			}
		}
	}

	return nil
}

func ExpectToVisit(values ...string) expectToVisit {
	m := make(map[string]int)
	for _, v := range values {
		m[v] = m[v] + 1
	}
	return expectToVisit{
		m, make(map[string]int),
	}
}

type expectToVisit struct {
	expectations map[string]int
	visited      map[string]int
}

func (e expectToVisit) Visit(_ string, value string) {
	e.visited[value] = e.visited[value] + 1
}

func (e expectToVisit) Error() error {
	if len(e.expectations) != len(e.visited) {
		return fmt.Errorf("Expected to visit %d unique values (%v), got %d (%v)", len(e.expectations), e.expectations, len(e.visited), e.visited)
	}

	for key, visitations := range e.visited {
		if e.expectations[key] != visitations {
			return fmt.Errorf("Expected to visit %s %d times, got %d", key, e.expectations[key], visitations)
		}
	}

	return nil
}
