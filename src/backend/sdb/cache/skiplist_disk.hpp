/*
 * In the implementation, the same memory pool technique is used as before to avoid memory fragmentation,
 * and the same disk-based strategy is used to ensure data persistence. To support reading and writing nodes
 * from disk, a size() function is added to the node struct in order to calculate the size of a node, and read/write
 * operations are encapsulated in the flush_nodes_to_disk() and flush_nodes_to_memory() functions.
 * 
 * In put(), when memory usage approaches the threshold, the program writes some nodes to disk to free up space
 * for new nodes. Specifically, the program attempts to reuse a node from the free node pool, and if none is available,
 * allocates a new one. It then generates a random number to determine the level of the new node, creates the new node,
 * and inserts it into the skip list. If the number of nodes exceeds the value of num_nodes_to_flush_, all nodes are
 * written to disk by calling flush_nodes_to_disk(). For thread safety, put() first acquires a shared lock and then
 * upgrades to an exclusive lock when modification of nodes is needed.
 * 
 * In get(), when the specified key-value pair is not in memory, the program reads the corresponding node from disk.
 * Specifically, the program first searches for the predecessor node of the specified key-value pair, and then finds
 * the successor node through the next[0] pointer of the predecessor node. If the successor node exists, the key-value
 * pair has already been inserted into the skip list, and the program returns the value of the node. Otherwise, the
 * program needs to read the node from disk. For performance reasons, the program reads all nodes from disk into memory
 * and manages these nodes using the allocated_nodes_ vector. When executing get() operation, the program only needs to
 * search for the corresponding node in memory. For thread safety, get() first acquires a shared lock.
 * 
 * In remove(), the program first finds the predecessor node of the node to be deleted and modifies the predecessor's
 * next[] pointer. Then it adds the node to be deleted to the free node pool for reuse. If the number of nodes is below
 * the value of num_nodes_to_flush_threshold_, the program calls flush_nodes_to_memory() to read all nodes from disk.
 * Unlike put() and get(), remove() requires an exclusive lock to ensure exclusive access.
 * 
 * In clear(), the program deletes all nodes in memory and the file on disk, and resets the counters num_nodes_ and
 * num_nodes_to_flush_ to 0. When executing clear() operation, the program needs to acquire an exclusive lock to prevent
 * interference from other operations.
 */

_Pragma("once")

#include "cache.hpp"

#include <random>
#include <fstream>
#include <vector>
#include <algorithm>
#include <shared_mutex>

namespace sdb {

// Disk-based skiplist implementation
template<typename K, typename V>
class DiskSkipList : public Disk<K, V> {
public:
    DiskSkipList() = default;
	~DiskSkipList() = default;

	void InitDisk(const std::string& path, SDBContext *cxt) override {
		filename_ = path;
		max_level_ = 32;
		p_ = 0.25;
		nodes_to_flush_ = 10000;
        num_nodes_to_flush_threshold_ = 20000;
		num_nodes_ = 0;
		file_size_ = 0;
		head_ = new_node(K(), V(), max_level_);
        for (uint32_t i = 0; i < max_level_; ++i) {
            head_->next[i] = 0;
		}
	}

    ~DiskSkipList() {
        Clear(nullptr);
        delete head_;
    }

    void Put(const K &key, const V &value, SDBContext *cxt) override {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        uint64_t update[max_level_];
        Node *x = find_predecessors(key, update);
        if (x && x->key == key) { // 若存在则更新
            x->value = value;
            return;
        }
        Node *node = new_node_with_level(key, value, random_level());
        ++num_nodes_;
        for (uint32_t i = 0; i < node->level; ++i) {
            node->next[i] = x->next[i];
            x->next[i] = node_to_offset(node);
        }

        if (num_nodes_ >= num_nodes_to_flush_threshold_) {
            flush_nodes_to_disk();
        } else if (num_nodes_ >= num_nodes_to_flush_) {
            num_nodes_to_flush_ += nodes_to_flush_;
            flush_nodes_to_disk();
        }
    }

    bool Get(const K &key, V &value, SDBContext *cxt) override {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        Node *x = find(key);
        if (x) {
            value = x->value;
            return true;
        }
        return false;
    }

    void Delete(const K &key, SDBContext *cxt) override {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        uint64_t update[max_level_];
        Node *x = find_predecessors(key, update);
        if (!x || x->key != key) {
            return;
		}

        for (uint32_t i = 0; i < x->level; ++i) {
            if (update[i] == node_to_offset(x)) {
                head_->next[i] = x->next[i];
            } else {
                offset_to_node(update[i])->next[i] = x->next[i];
			}
        }
        free_node(x);
        --num_nodes_;

        if (num_nodes_ < num_nodes_to_flush_threshold_) {
            flush_nodes_to_memory();
        } else if (num_nodes_ < num_nodes_to_flush_ - nodes_to_flush_) {
            flush_nodes_to_memory();
        } else if (num_nodes_ == 0) {
            flush_nodes_to_disk();
        }
    }

    void Clear(SDBContext *cxt) override {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        num_nodes_ = 0;
        num_nodes_to_flush_ = nodes_to_flush_;
        allocated_nodes_.clear();
        freed_nodes_.clear();
        head_->level = max_level_;
        for (uint32_t i = 0; i < max_level_; ++i)
            head_->next[i] = 0;
        file_size_ = 0;
        std::ofstream fout(filename_, std::ios::binary | std::ios::trunc);
        fout.close();
    }

private:
    static constexpr double kBranching = 0.5;

    struct Node {
        K key;
        V value;
        uint32_t level;
        uint64_t next[1];

        static uint64_t size(uint32_t level) {
            return sizeof(Node) + (level - 1) * sizeof(uint64_t);
        }
    };

    std::string filename_;
    std::vector<Node *> allocated_nodes_;
    std::vector<Node *> freed_nodes_;
    uint32_t max_level_;
    double p_;
    uint32_t nodes_to_flush_;
    uint32_t num_nodes_to_flush_threshold_;
    uint64_t num_nodes_;
    uint64_t num_nodes_to_flush_;
    uint64_t file_size_;
    Node *head_;
    mutable std::shared_mutex mutex_;

    inline uint64_t node_to_offset(Node *node) const {
        return reinterpret_cast<uint64_t>(node) - reinterpret_cast<uint64_t>(allocated_nodes_[0]);
    }

    inline Node *offset_to_node(uint64_t offset) const {
        return reinterpret_cast<Node *>(reinterpret_cast<uint64_t>(&allocated_nodes_[0]) + offset);
    }

    inline Node *new_node(const K &key, const V &value, uint32_t level) {
        if (freed_nodes_.empty()) { // 如果没有可重用的节点，则新分配一个
            uint64_t len = Node::size(level);
            allocated_nodes_.push_back(reinterpret_cast<Node *>(new char[len]));
        }
        Node *node = freed_nodes_.empty() ? allocated_nodes_.back() : freed_nodes_.back();
        node->key = key;
        node->value = value;
        node->level = level;
        for (uint32_t i = 0; i < level; ++i) {
            node->next[i] = 0;
		}
        if (!freed_nodes_.empty()) {
            freed_nodes_.pop_back();
		}
        return node;
    }
    inline Node *new_node_with_level(const K &key, const V &value, uint32_t level) {
        Node *node = new_node(key, value, level);
        node->level = std::min(level, max_level_);
        return node;
    }

inline void free_node(Node *node) {
    freed_nodes_.push_back(node);
}

uint32_t random_level() {
    static std::mt19937 generator(std::random_device{}());
    static std::uniform_real_distribution<double> distribution(0.0, 1.0);
    uint32_t level = 1;
    while (distribution(generator) < p_ && level < max_level_) {
        ++level;
	}
    return level;
}

Node *find(const K &key) const {
    Node *x = head_;
    for (uint32_t i = max_level_; i-- > 0;) {
        while (x->next[i]) {
            Node *y = offset_to_node(x->next[i]);
            if (y->key < key) {
                x = y;
            } else if (y->key == key) {
                return y;
            } else {
                break;
            }
        }
    }
    return nullptr;
}

Node *find_predecessors(const K &key, uint64_t *update) const {
    Node *x = head_;
    for (uint32_t i = max_level_; i-- > 0;) {
        while (x->next[i]) {
            Node *y = offset_to_node(x->next[i]);
            if (y->key < key) {
                x = y;
            } else {
                break;
            }
        }
        update[i] = node_to_offset(x);
    }
    return offset_to_node(x->next[0]);
}

void flush_nodes_to_disk() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    std::ofstream fout(filename_, std::ios::binary | std::ios::app);
    if (fout.bad()) {
        return;
	}
    Node *x = head_->next[0];
    while (x) {
        fout.write(reinterpret_cast<const char *>(x), Node::size(x->level));
        x = x->next[0] ? offset_to_node(x->next[0]) : nullptr;
    }
    file_size_ = fout.tellp(); // 更新文件大小
    num_nodes_to_flush_ += nodes_to_flush_;
    fout.close();
}

void flush_nodes_to_memory() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    std::ifstream fin(filename_, std::ios::binary);
    if (fin.bad()) {
        return;
	}
    clear();
    fin.seekg(0, std::ios::end);
    file_size_ = fin.tellg();
    num_nodes_ = file_size_ / Node::size(1);
    allocated_nodes_.resize(num_nodes_);
    fin.seekg(0, std::ios::beg);
    for (uint64_t i = 0; i < num_nodes_; ++i) {
        Node *node = reinterpret_cast<Node *>(&allocated_nodes_[0] + i * Node::size(1));
        fin.read(reinterpret_cast<char *>(node), sizeof(Node));
        for (uint32_t j = 0; j < node->level; ++j) {
            fin.read(reinterpret_cast<char *>(&node->next[j]), sizeof(uint64_t));
		}
    }
    fin.close();
    num_nodes_to_flush_threshold_ = nodes_to_flush_;
}
};

} // namespace sdb