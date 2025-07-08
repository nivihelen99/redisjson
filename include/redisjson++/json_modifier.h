#pragma once

#include "path_parser.h" // Uses PathElement
#include "exceptions.h"   // For custom exceptions
#include <nlohmann/json.hpp>
#include <string>
#include <vector>
#include <variant> // Added for std::variant

using json = nlohmann::json;

namespace redisjson {

// Forward declare MergeStrategy if it's complex, or define here if simple
enum class MergeStrategy {
    SHALLOW,     // Only merge top-level keys
    DEEP,        // Recursively merge all levels
    OVERWRITE,   // Overwrite existing values (json::update does this by default for objects)
    APPEND,      // Append to arrays, merge objects (requires custom logic)
    PATCH        // Apply RFC 6902 JSON Patch (requires a library or complex implementation)
};


class JSONModifier {
public:
    // Basic Operations
    /**
     * Gets the JSON value at the specified path.
     * Throws PathNotFoundException if the path does not exist.
     * Throws TypeMismatchException if path leads to a different type than expected by context (though `get` itself is generic).
     * Throws InvalidPathException for issues with path structure during traversal.
     */
    json get(const json& document, const std::vector<PathParser::PathElement>& path_elements) const;

    /**
     * Sets the JSON value at the specified path.
     * If intermediate paths do not exist, they will be created (objects for keys, arrays for indices if specified or clear).
     * This behavior might need refinement (e.g. create_path option).
     * Throws TypeMismatchException if trying to set a key on a non-object or index on a non-array.
     * Throws InvalidPathException for issues with path structure.
     */
    void set(json& document, const std::vector<PathParser::PathElement>& path_elements,
             const json& value_to_set, bool create_path = true, bool overwrite = true);

    /**
     * Deletes the JSON value or element at the specified path.
     * Throws PathNotFoundException if the path does not exist.
     * Throws TypeMismatchException if trying to delete from a non-object/non-array parent.
     * Throws IndexOutOfBoundsException for invalid array indices.
     */
    void del(json& document, const std::vector<PathParser::PathElement>& path_elements);

    // Advanced Operations (Stubs for now)
    /**
     * Merges the 'patch' document into the 'document' according to the specified strategy.
     */
    void merge(json& document, const json& patch, MergeStrategy strategy = MergeStrategy::DEEP);

    /**
     * Applies a JSON Patch (RFC 6902) to the document.
     */
    void apply_patch(json& document, const json& patch_operations); // patch_operations is an array of patch ops

    /**
     * Generates a JSON Patch (RFC 6902) representing the difference between old_doc and new_doc.
     */
    json diff(const json& old_doc, const json& new_doc) const;

    // Array Operations (Stubs for now)
    void array_append(json& document, const std::vector<PathParser::PathElement>& path_elements,
                      const json& value_to_append);
    void array_prepend(json& document, const std::vector<PathParser::PathElement>& path_elements,
                       const json& value_to_prepend);
    json array_pop(json& document, const std::vector<PathParser::PathElement>& path_elements,
                   int index = -1); // -1 for last element
    void array_insert(json& document, const std::vector<PathParser::PathElement>& path_elements,
                      int index, const json& value_to_insert);
    long long array_trim(json& document, const std::vector<PathParser::PathElement>& path_elements,
                         long long start, long long stop);
    // size_t array_length(const json& document, const std::vector<PathParser::PathElement>& path_elements) const; // In requirements but might be better in get_size or similar

    // Utility Operations
    /**
     * Checks if a path exists in the document.
     */
    bool exists(const json& document, const std::vector<PathParser::PathElement>& path_elements) const;

    /**
     * Gets the JSON type of the value at the specified path.
     * Throws PathNotFoundException if path does not exist.
     */
    json::value_t get_type(const json& document,
                           const std::vector<PathParser::PathElement>& path_elements) const;

    /**
     * Gets the size of the element at the path.
     * For objects, number of keys. For arrays, number of elements. For strings, length.
     * For other types, typically 1 or 0 (or throw).
     * Throws PathNotFoundException if path does not exist.
     */
    size_t get_size(const json& document,
                    const std::vector<PathParser::PathElement>& path_elements) const;

private:
    // Helper to navigate to the parent of the target path element and return a pointer/reference
    // Returns a pointer to the parent json object/array.
    // The last element of path_elements is the target key/index.
    // `final_key_or_index` will hold the last segment (string key or int index).
    json* navigate_to_parent(json& doc,
                             const std::vector<PathParser::PathElement>& path_elements,
                             std::variant<std::string, int>& final_key_or_index,
                             bool create_missing_paths = false) const;

    const json* navigate_to_parent_const(const json& doc,
                                         const std::vector<PathParser::PathElement>& path_elements,
                                         std::variant<std::string, int>& final_key_or_index) const;

    // Overload for navigating to the element itself, not parent
     json* navigate_to_element(json& doc,
                               const std::vector<PathParser::PathElement>& path_elements,
                               bool create_missing_paths = false) const;

    const json* navigate_to_element_const(const json& doc,
                                          const std::vector<PathParser::PathElement>& path_elements) const;
};

} // namespace redisjson
