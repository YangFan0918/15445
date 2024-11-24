#include "primer/trie.h"
#include <cassert>
#include <cstddef>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>
#include "common/exception.h"
namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");
  auto r = root_;
  if (r == nullptr) {
    return nullptr;
  }
  for (auto now_char : key) {
    if (r->children_.count(now_char)) {
      r = (r->children_.at(now_char));
    } else {
      return nullptr;
    }
  }
  auto res = std::dynamic_pointer_cast<const TrieNodeWithValue<T>>(r);
  if (res) {
    return res->value_.get();
  }
  return nullptr;
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");
  std::shared_ptr<TrieNode> rt;
  std::shared_ptr<TrieNode> pre;
  if (root_) {
    rt = pre = root_->Clone();
  } else {
    rt = pre = std::make_shared<TrieNode>();
  }
  int n = key.size();
  if (!n) {
    if (!root_) {
      return Trie(std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value))));
    }
    return Trie(
        std::make_shared<TrieNodeWithValue<T>>(root_->Clone()->children_, std::make_shared<T>(std::move(value))));
  }
  for (int i = 0; i < n - 1; i++) {
    if (pre->children_.count(key[i])) {
      std::shared_ptr<TrieNode> ne = pre->children_.at(key[i])->Clone();
      pre->children_[key[i]] = ne;
      pre = ne;
    } else {
      std::shared_ptr<TrieNode> ne = std::make_shared<TrieNode>();
      pre->children_[key[i]] = ne;
      pre = ne;
    }
  }
  if (pre->children_.count(key[n - 1])) {
    auto mp = pre->children_.at(key[n - 1])->children_;
    std::shared_ptr<TrieNodeWithValue<T>> ne =
        std::make_shared<TrieNodeWithValue<T>>(mp, std::make_shared<T>(std::move(value)));
    pre->children_[key[n - 1]] = ne;
  } else {
    std::shared_ptr<TrieNodeWithValue<T>> ne =
        std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
    pre->children_[key[n - 1]] = ne;
    assert(ne);
  }
  return Trie(rt);
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");
  std::shared_ptr<TrieNode> rt;
  std::shared_ptr<TrieNode> pre;
  if (root_) {
    rt = pre = root_->Clone();
  } else {
    return Trie(nullptr);
  }
  if (key.empty()) {
    if (root_) {
      return Trie(std::make_shared<TrieNode>((root_->Clone()->children_)));
    }
    return Trie(std::make_shared<TrieNode>());
  }
  std::vector<std::pair<std::shared_ptr<TrieNode>, char>> ss;
  int n = key.size();
  for (int i = 0; i < n - 1; i++) {
    ss.emplace_back(std::make_pair(pre, key[i]));
    if (pre->children_.count(key[i]) != 0U) {
      std::shared_ptr<TrieNode> ne = pre->children_.at(key[i])->Clone();
      pre->children_[key[i]] = ne;
      pre = ne;
    } else {
      std::shared_ptr<TrieNode> ne = std::make_shared<TrieNode>();
      pre->children_[key[i]] = ne;
      pre = ne;
    }
  }
  ss.emplace_back(std::make_pair(pre, key[n - 1]));
  std::shared_ptr<TrieNode> ne = std::make_shared<TrieNode>(pre->children_[key[n - 1]]->children_);
  pre->children_[key[n - 1]] = ne;
  reverse(ss.begin(), ss.end());
  for (auto [x, y] : ss) {
    if (x->children_[y]->children_.empty()) {
      x->children_.erase(y);
      if (x->is_value_node_) {
        break;
      }
    } else {
      break;
    }
  }
  if (rt->children_.empty() && (!rt->is_value_node_)) {
    rt = nullptr;
  }
  return Trie(rt);
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
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
