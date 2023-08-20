#pragma once

#include <vector>
#include <queue>
#include <mutex>
#include <memory>
#include <condition_variable>

namespace sdb {

template <typename T>
class SharedMutliQueue {
public:
  SharedMutliQueue() {
  }

  void Reset(size_t n) {
    std::deque<T> tmp;
    queues_.assign(n, tmp);
  }

  ~SharedMutliQueue() = default;

  T pop_front_any(size_t* i);
  bool try_pop_front_any(T*, size_t* i);

  T pop_front(size_t i);
  bool try_pop_front(size_t i, T*);

  void push_back(size_t i, const T& item);
  void push_back(size_t i, T&& item);

  int size();
  bool empty();

private:
  std::vector<std::deque<T> > queues_;
  std::mutex mutex_;
  std::condition_variable cond_;
}; 

template <typename T>
T SharedMutliQueue<T>::pop_front_any(size_t* route) {
  std::unique_lock<std::mutex> mlock(mutex_);

  while (true) {
    for (int i = 0; i < queues_.size(); ++i) {
      std::deque<T>& queue = queues_[i];
      if (queue.empty()) {
        continue;
      }
      auto t = queue.front();
      queue.pop_front();
      *route = i;
      return t;
    }
    cond_.wait(mlock);
  }
}     

template <typename T>
bool SharedMutliQueue<T>::try_pop_front_any(T* t, size_t* route) {
  std::unique_lock<std::mutex> mlock(mutex_);
  for (int i = 0; i < queues_.size(); ++i) {
    std::deque<T>& queue = queues_[i];
    if (queue.empty()) {
      continue;
    }
    *route = i;
    *t = queue.front();
    queue.pop_front();
    return true;
  }
  return false;
}

template <typename T>
T SharedMutliQueue<T>::pop_front(size_t i) {
  std::unique_lock<std::mutex> mlock(mutex_);
  std::deque<T>& queue = queues_[i];
  if (queue.empty()) {
    cond_.wait(mlock);
  }
  auto t = queue.front();
  queue.pop_front();
  return t;
}     

template <typename T>
bool SharedMutliQueue<T>::try_pop_front(size_t i, T* t) {
  std::unique_lock<std::mutex> mlock(mutex_);
  std::deque<T>& queue = queues_[i];
  if (queue.empty()) {
    return false;
  }
  *t = queue.front();
  queue.pop_front();
  return true;
}


template <typename T>
void SharedMutliQueue<T>::push_back(size_t i, const T& item) {
  std::unique_lock<std::mutex> mlock(mutex_);
  std::deque<T>& queue = queues_[i];
  queue.push_back(item);
  mlock.unlock();     // unlock before notificiation to minimize mutex con
  cond_.notify_one(); // notify one waiting thread
}

template <typename T> 
void SharedMutliQueue<T>::push_back(size_t i, T&& item) {
  std::unique_lock<std::mutex> mlock(mutex_);
  std::deque<T>& queue = queues_[i];
  queue.push_back(std::move(item));
  mlock.unlock();     // unlock before notificiation to minimize mutex con
  cond_.notify_one(); // notify one waiting thread
}

template <typename T>
int SharedMutliQueue<T>::size() {
  std::unique_lock<std::mutex> mlock(mutex_);
  int size = 0;
  for (int i = 0; i < queues_.size(); ++i) {
    std::deque<T>& queue = queues_[i];
    size += queue.size();
  }
  mlock.unlock();
  return size;
}

}
