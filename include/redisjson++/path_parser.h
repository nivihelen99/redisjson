#pragma once

#include <string>
#include <vector>
#include <nlohmann/json.hpp> // For json type if needed in methods

namespace redisjson {

// Forward declare json if preferred over including nlohmann/json.hpp directly
// using json = nlohmann::json;

class PathParser {
public:
    PathParser() = default; // Default constructor

    // Based on requirement.md
    struct PathElement {
        enum class Type { KEY, INDEX, SLICE, WILDCARD, FILTER, RECURSIVE };
        std::string key_name; // Changed from key
        int index = -1;
        int start = -1, end = -1;
        std::string filter_expression;
        Type type;
        bool is_array_element = false; // Changed from is_array_operation
    };

    // Declarations
    std::vector<PathElement> parse(const std::string& path) const;
    bool is_valid_path(const std::string& path) const;
    std::string normalize_path(const std::string& path) const;
    std::vector<std::string> expand_wildcards(const nlohmann::json& document,
                                             const std::string& path) const;

    // Static helpers
    static bool is_root_path(const std::string& path_str);
    // Determines if the path implies the target should be an array, useful for creation logic.
    // This is a heuristic. A path like "a.b" doesn't strictly define 'b' as an array
    // until an operation like "a.b[0]" or "a.b.append(...)" is attempted.
    // `path_elements` is the path to the potential array.
    // `doc_context` is the document up to the parent of the potential array.
    static bool is_array_path(const std::vector<PathElement>& path_elements_to_target, const nlohmann::json& doc_context);
    static std::string escape_key_if_needed(const std::string& key_name);
    static std::string reconstruct_path(const std::vector<PathElement>& path_elements);


private:
    // Helper for wildcard expansion, if needed to be distinct from public API
    std::vector<std::string> expand_wildcards(const nlohmann::json& document,
                                             const std::vector<PathElement>& parsed_path) const;
};

} // namespace redisjson
