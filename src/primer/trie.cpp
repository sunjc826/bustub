#include "primer/trie.h"
#include <memory>
#include <string_view>
#include "common/exception.h"
#include <stack>

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
    if (root_ == nullptr)
    {
        return nullptr;
    }

    auto node = root_;

    for (char ch : key)
    {
        auto it = node->children_.find(ch);
        if (it == node->children_.end()) 
        {
            return nullptr;
        }
        node = it->second;
    }
    if (not node->is_value_node_) 
    {
        return nullptr;
    }
    auto node_with_value = dynamic_cast<TrieNodeWithValue<T const> const *>(node.get());
    if (node_with_value == nullptr) 
    {
        return nullptr;
    }
    return node_with_value->value_.get();
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
    std::stack<std::shared_ptr<TrieNode const>> path;
    
    auto node = root_;
    if (node == nullptr) {
        // This is a simplification
        // We could possibly make this more performant by having more complicated logic
        // and not having this unnecessary allocation
        node = std::make_shared<TrieNode>();
    }
    path.push(node);
    size_t idx;
    for (idx = 0; idx < key.size(); ++idx)
    {
        auto it = node->children_.find(key[idx]);
        if (it == node->children_.end())
        {
            break;
        }
        node = it->second;
        path.push(node);
    }

    std::shared_ptr<TrieNode const> chain = nullptr;
    if (idx == key.size())
    {
        chain = std::make_shared<TrieNodeWithValue<T const> const>(node->children_, std::make_shared<T>(std::move(value)));
        path.pop();
    }
    else {
        chain = std::make_shared<TrieNodeWithValue<T const> const>(std::make_shared<T>(std::move(value)));
        for (size_t i = key.size(); i --> idx + 1;)
        {
            chain = std::make_shared<TrieNode const>(std::map<char, std::shared_ptr<TrieNode const>>({{key[i], chain}}));
        }
    }

    // this min is needed in the case where idx = key.size()
    for (size_t i = std::min(idx + 1, key.size()); i --> 0;)
    {
        auto top = path.top()->Clone();
        path.pop();
        top->children_.insert_or_assign(key[i], chain);
        chain = std::move(top);
    }
    
    return Trie{chain};
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
    if (root_ == nullptr)
    {
        return Trie{};
    }
    std::stack<std::shared_ptr<TrieNode const>> path;
    auto node = root_;
    path.push(root_);
    for (char ch : key)
    {
        auto it = node->children_.find(ch);
        if (it == node->children_.end())
        {
            return *this;
        }
        node = it->second;
        path.push(node);
    }

    if (not node->is_value_node_)
    {
        return *this;
    }
    std::shared_ptr<TrieNode const> chain = nullptr;
    if (node->children_.empty())
    {
        // go back up the path until we find a node that either has value, or has more than 1 child
        path.pop();
        while (not path.empty())
        {
            node = path.top();
            if (node->is_value_node_ or node->children_.size() > 1)
            {
                break;
            }
            path.pop();
        }
        if (path.empty())
        {
            return Trie{};
        }
                 
        auto node_copy = node->Clone();
        node_copy->children_.erase(key[path.size() - 1]);
        chain = std::move(node_copy);
        path.pop();
    }
    else 
    {
        chain = node->TrieNode::Clone();
        path.pop();
    }
    
    for (size_t i = path.size(); i --> 0;)
    {
        auto top = path.top()->Clone();
        top->children_.insert_or_assign(key[i], chain);
        chain = std::move(top);
        path.pop();
    }

    return Trie{chain};
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
