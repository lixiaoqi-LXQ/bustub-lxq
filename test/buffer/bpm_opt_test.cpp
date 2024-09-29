#include "buffer/buffer_pool_manager.h"
#include "fmt/core.h"
#include "fmt/std.h"
#include "gtest/gtest.h"

#include <chrono>
#include <cstdio>
#include <limits>
#include <random>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;


namespace bustub {

TEST(BufferPoolManagerOptimizationTest, NewPageOnlyTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 64;
  const size_t k = 2;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager, k);

  std::vector<std::thread> threads;

  for (int i = 0; i < 2; ++i) {
    threads.emplace_back([&bpm, i] {
      page_id_t page_id;
      auto start = std::chrono::high_resolution_clock::now();
      while (true) {
        auto *page = bpm->NewPage(&page_id);
        if (page != nullptr) {
          fmt::print("thread {} get new page with page id {}\n", i, page_id);
          bpm->UnpinPage(page_id, true);
        } else {
        }

        // time
        if (std::chrono::high_resolution_clock::now() - start > 1s) {
          break;
        }
      }
      // page_ids.push_back(page_id);
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}
}  // namespace bustub