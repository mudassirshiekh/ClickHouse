#pragma once

#include <vector>
#include <optional>
#include <string_view>

namespace DB
{

class CharacterFinder
{
public:
    using Position = std::size_t;

    virtual ~CharacterFinder() = default;

    static std::optional<Position> find_first(std::string_view haystack, const std::vector<char> & needles);

    static std::optional<Position> find_first(std::string_view haystack, std::size_t offset, const std::vector<char> & needles);

    static std::optional<Position> find_first_not(std::string_view haystack, const std::vector<char> & needles);

    static std::optional<Position> find_first_not(std::string_view haystack, std::size_t offset, const std::vector<char> & needles);

};

/*
 * Maybe decorator would be better :)
 * */
class BoundsSafeCharacterFinder
{
    using Position = std::size_t;
public:
    std::optional<Position> find_first(std::string_view haystack, std::size_t offset, const std::vector<char> & needles) const;

    std::optional<Position> find_first_not(std::string_view haystack, std::size_t offset, const std::vector<char> & needles) const;
};

}

