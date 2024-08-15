#include "primer/trie.h"
#include <cassert>
#include <stack>
#include <string_view>
#include "common/exception.h"

using bustub::TrieNode;
using bustub::TrieNodeWithValue;
using NodePtr_S = std::shared_ptr<const TrieNode>;

template <class T>
NodePtr_S ConstructTreeBranch(std::string_view key, std::shared_ptr<T> val_shared_ptr) {
  std::map<char, NodePtr_S> children;
  if (key.length() == 0) {
    return std::make_shared<TrieNodeWithValue<T>>(children, val_shared_ptr);
  }
  auto child = ConstructTreeBranch(key.substr(1), val_shared_ptr);
  children.emplace(key.at(0), child);
  return std::make_shared<TrieNode>(children);
}

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  if (root_ == nullptr) {
    return nullptr;
  }
  auto node_ptr = root_;
  decltype(node_ptr->children_.begin()) it;
  for (const auto &c : key) {
    it = node_ptr->children_.find(c);
    if (it == node_ptr->children_.end()) {
      return nullptr;
    }
    node_ptr = it->second;
  }

  // if (not node_ptr->is_value_node_) return nullptr;

  auto ptr = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(node_ptr);
  if (ptr == nullptr) {
    return nullptr;
  }

  return ptr->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T val) const -> Trie {
  auto val_shared_ptr = std::make_shared<T>(std::move(val));

  // empty trie
  if (root_ == nullptr) {
    return Trie(ConstructTreeBranch<T>(key, val_shared_ptr));
  }

  std::stack<std::pair<NodePtr_S, char>> pointers;
  NodePtr_S new_tree{nullptr};

  auto node_ptr = root_;
  for (unsigned i = 0; i < key.length(); i++) {
    char c = key[i];
    pointers.push({node_ptr, c});
    auto it = node_ptr->children_.find(c);
    if (it == node_ptr->children_.end()) {
      new_tree = ConstructTreeBranch<T>(key.substr(i + 1), val_shared_ptr);
      break;
    }
    node_ptr = it->second;
  }

  // key found
  if (new_tree == nullptr) {
    auto children = node_ptr->children_;
    new_tree = std::make_shared<TrieNodeWithValue<T>>(children, val_shared_ptr);
  }

  while (not pointers.empty()) {
    auto [parent, c] = pointers.top();
    pointers.pop();
    auto parent_copy = parent->Clone();
    parent_copy->children_[c] = new_tree;
    new_tree = NodePtr_S(std::move(parent_copy));
  }
  return Trie(new_tree);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  std::stack<std::pair<NodePtr_S, char>> pointers;

  auto node_ptr = root_;
  for (auto c : key) {
    pointers.push({node_ptr, c});
    auto it = node_ptr->children_.find(c);
    if (it == node_ptr->children_.end()) {
      return *this;
    }
    node_ptr = it->second;
  }

  if (not node_ptr->is_value_node_) {
    return *this;
  }

  auto &children = node_ptr->children_;
  NodePtr_S new_tree = children.empty() ? nullptr : std::make_shared<TrieNode>(children);

  while (not pointers.empty()) {
    auto [parent, c] = pointers.top();
    pointers.pop();
    auto parent_copy = parent->Clone();
    if (new_tree != nullptr) {
      parent_copy->children_[c] = new_tree;
    } else {
      parent_copy->children_.erase(c);
    }

    if (not parent_copy->children_.empty() or parent_copy->is_value_node_) {
      new_tree = std::move(parent_copy);
    } else {
      new_tree = nullptr;
    }
  }
  return Trie(new_tree);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
