#include "earcut.h"
#include <array>
#include <cstdlib>
#include <cstring>
#include <mapbox/earcut.hpp>

int32_t comparison_earcut(const double *coordinates, const int32_t *ringLengths, int32_t ringCount, uint32_t **indices) {
    std::vector<std::vector<std::array<double, 2>>> polygon;
    for (int32_t i = 0; i < ringCount; ++i) {
        auto &ring = polygon.emplace_back();
        for (int32_t j = 0; j < ringLengths[i]; ++j, coordinates += 2) {
            ring.push_back({coordinates[0], coordinates[1]});
        }
    }
    auto result = mapbox::earcut<uint32_t>(polygon);
    if (result.empty()) return 0;
    *indices = static_cast<uint32_t *>(std::malloc(result.size() * sizeof(uint32_t)));
    if (!*indices) return 0;
    std::memcpy(*indices, result.data(), result.size() * sizeof(uint32_t));
    return static_cast<int32_t>(result.size());
}
