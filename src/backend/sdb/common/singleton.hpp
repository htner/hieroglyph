#pragma once
#include <iostream>
#include <mutex>
#include <cassert>

template<class T>
class ThreadSafeSingleton {

public:
  template<typename...Args>
  static T* GetInstance(Args&&... args) {
    std::call_once(ThreadSafeSingleton<T>::call_once_flag_,
                   std::forward<void(Args&&...)>(&ThreadSafeSingleton<T>::Init),
                   std::forward<Args>(args)...);
    return ThreadSafeSingleton<T>::instance_;
  }

private:
  ThreadSafeSingleton() = default;

public:
  ~ThreadSafeSingleton() {
    delete ThreadSafeSingleton<T>::instance_;
    ThreadSafeSingleton<T>::instance_ = nullptr;
  }
  ThreadSafeSingleton(const ThreadSafeSingleton& o) = delete;
  ThreadSafeSingleton& operator=(const ThreadSafeSingleton& o) = delete;

  template<typename...Args>
  static void Init(Args&&...args) {
    instance_ =  new T(std::forward<Args>(args)...);
  }

private:
  static std::once_flag call_once_flag_;
  static T* instance_;
};

template<class T> T* ThreadSafeSingleton<T>::instance_ = nullptr;
template<class T> std::once_flag ThreadSafeSingleton<T>::call_once_flag_;
