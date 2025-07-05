#include "redisjson++/path_parser.h"
#include "redisjson++/exceptions.h" // For InvalidPathException
#include <nlohmann/json.hpp> // Added for json type
#include <iostream> // For temporary debugging
#include <algorithm>
#include <stdexcept> // For std::invalid_argument, std::out_of_range
#include <optional> // Required for PathElement slice members

namespace redisjson {

using json = nlohmann::json; // Added using directive

// Basic helper to trim whitespace
static std::string trim(const std::string& str) {
    const std::string WHITESPACE = " \n\r\t\f\v";
    size_t start = str.find_first_not_of(WHITESPACE);
    if (start == std::string::npos) return ""; // Empty or all whitespace
    size_t end = str.find_last_not_of(WHITESPACE);
    return str.substr(start, (end - start + 1));
}

// Helper for reconstruction logic in expand_wildcards
static bool elements_before_require_dot(const PathParser::PathElement& prev_el) {
    // A key usually follows a dot, unless it's the first element
    // or follows a bracket access that implies an object.
    // If previous was an index into an array, and current is a key, it implies object in array.
    // e.g. array[0].key
    return prev_el.type == PathParser::PathElement::Type::KEY ||
           (prev_el.type == PathParser::PathElement::Type::INDEX && prev_el.is_array_element); // Ensure it's an array index leading to an object key
}


// Very simplified initial parser. It will only handle basic dot notation and simple array indexing.
// e.g., "key1.key2", "array[0].key"
// Does not yet support root '$', wildcards, slices, filters, recursive descent, or complex bracket notations.
std::vector<PathParser::PathElement> PathParser::parse(const std::string& path_str_in) const {
    std::vector<PathElement> elements;
    std::string path_str = trim(path_str_in);

    if (path_str.empty()) {
        // "" is not a valid path, but "$" is.
        // Current parser returns empty for empty string. This might be okay if client ensures non-empty paths.
        // Or, throw InvalidPathException for empty string.
        // For now, let's assume an empty path string might mean "current node" or is invalid contextually.
        // If it's meant to be root, it should be "$".
        // Returning empty elements for empty string.
        return elements;
    }

    if (path_str == "$") {
        // Represent root path, perhaps with a special PathElement or by convention (empty elements list)
        // For now, an empty elements list might signify root, or a specific root element type.
        // Let's return empty, and callers can check path_str == "$" or elements.empty().
        // PathParser::is_root_path can encapsulate this.
        return elements;
    }


    std::string current_segment;
    for (size_t i = 0; i < path_str.length(); ++i) {
        char c = path_str[i];

        if (c == '.') {
            if (!current_segment.empty()) {
                PathElement elem;
                elem.type = PathElement::Type::KEY;
                elem.key_name = current_segment;
                elements.push_back(elem);
                current_segment.clear();
            } else if (elements.empty() && i == 0) {
                 throw InvalidPathException("Path cannot start with '.'");
            } else if (i > 0 && path_str[i-1] == '.') {
                 throw InvalidPathException("Path cannot contain '..'");
            }
        } else if (c == '[') {
            // Check if '[' is preceded by a '.'
            if (i > 0 && path_str[i-1] == '.') {
                throw InvalidPathException("Invalid path: '[' cannot immediately follow '.' (e.g. 'key.[0]')");
            }
            if (!current_segment.empty()) {
                PathElement elem;
                elem.type = PathElement::Type::KEY;
                elem.key_name = current_segment;
                elements.push_back(elem);
                current_segment.clear();
            } else if (elements.empty() && i != 0) { // '[' not at start and no preceding key segment like "obj" in "obj[0]"
                 // This implies path started with '[' but elements is not empty, which is contradictory.
                 // Or, current_segment is empty, elements is not empty, and i != 0. E.g. "[0][1]" - after [0], current_segment is empty.
                 // This specific check: "elements.empty() && i != 0" seems problematic.
                 // It should be: if current_segment is empty, AND elements is empty, AND i != 0, it's an error like ". [".
                 // If current_segment is empty, AND elements is NOT empty (e.g. after [0] in [0][1]), it's fine.
                 // The original check "Invalid path: '[' must follow a key or be at the start for root array access."
                 // is only relevant if current_segment is empty AND elements is empty AND i != 0.
                 // This should be simplified: if current_segment is empty, it means '[' follows ']', or is at start.
                 // If it's at start (i==0), it's fine.
                 // If it follows ']' (e.g. [0][1]), it's fine.
                 // The error "must follow a key or be at start" is for when current_segment is empty,
                 // but there was no preceding key name parsed, and it's not at the start.
                 // Example: if path was ".[" - this is caught by "cannot start with ."
                 // Example: if path was "..[" - this is caught by "cannot contain .."
                 // This specific else-if might be redundant or misformulated.
                 // For now, keeping it as is, but the new dot-bracket check is more direct.
                 throw InvalidPathException("Invalid path: '[' must follow a key or be at the start for root array access.");
            }

            size_t closing_bracket = path_str.find(']', i);
            if (closing_bracket == std::string::npos) {
                throw InvalidPathException("Mismatched brackets in path: '[' without ']'");
            }

            std::string bracket_content = path_str.substr(i + 1, closing_bracket - i - 1);
            bracket_content = trim(bracket_content);

            if (bracket_content.empty()) {
                throw InvalidPathException("Empty brackets [] are not valid (use [*] for wildcard).");
            }

            if ((bracket_content.front() == '\'' && bracket_content.back() == '\'') ||
                (bracket_content.front() == '"' && bracket_content.back() == '"')) {
                if (bracket_content.length() < 2) {
                     throw InvalidPathException("Invalid quoted key in brackets.");
                }
                PathElement elem;
                elem.type = PathElement::Type::KEY;
                elem.key_name = bracket_content.substr(1, bracket_content.length() - 2);
                if (elem.key_name.empty() && bracket_content.length() == 2) {
                    throw InvalidPathException("Empty quoted key name in path is not allowed.");
                }
                elements.push_back(elem);

            } else {
                try {
                    size_t chars_processed = 0;
                    int index_val = std::stoi(bracket_content, &chars_processed);
                    if (chars_processed != bracket_content.length()) {
                         throw InvalidPathException("Invalid characters in array index: " + bracket_content);
                    }
                    PathElement elem;
                    elem.type = PathElement::Type::INDEX;
                    elem.index = index_val;
                    elem.is_array_element = true; // This member name is now correct
                    elements.push_back(elem);
                } catch (const std::invalid_argument& e) {
                    throw InvalidPathException("Invalid array index (not a number): " + bracket_content);
                } catch (const std::out_of_range& e) {
                    throw InvalidPathException("Array index out of range: " + bracket_content);
                }
            }
            i = closing_bracket;
        } else {
            if (c == ']' ) {
                throw InvalidPathException("Mismatched brackets in path: ']' without '['");
            }
            current_segment += c;
        }
    }

    if (!current_segment.empty()) {
        PathElement elem;
        elem.type = PathElement::Type::KEY;
        elem.key_name = current_segment;
        elements.push_back(elem);
    } else if (!path_str.empty() && (path_str.back() == '.')) {
        throw InvalidPathException("Path cannot end with '.'");
    }

    return elements;
}

bool PathParser::is_valid_path(const std::string& path_str) const {
    try {
        parse(path_str);
        return true;
    } catch (const InvalidPathException&) {
        return false;
    }
    catch (const std::exception&) {
        return false;
    }
}

std::string PathParser::normalize_path(const std::string& path_str) const {
    if (!is_valid_path(path_str)) {
        throw InvalidPathException("Cannot normalize an invalid path: " + path_str);
    }
    return path_str;
}

std::vector<std::string> PathParser::expand_wildcards(const json& document,
                                                      const std::vector<PathElement>& parsed_path) const {
    bool has_wildcard = false;
    for(const auto& el : parsed_path) {
        if (el.type == PathElement::Type::WILDCARD ||
            el.type == PathElement::Type::RECURSIVE ||
            el.type == PathElement::Type::FILTER) {
            has_wildcard = true;
            break;
        }
    }

    if (!has_wildcard) {
        std::string reconstructed_path;
        for (size_t i = 0; i < parsed_path.size(); ++i) {
            const auto& el = parsed_path[i];
            if (el.type == PathElement::Type::KEY) {
                if (i > 0 && elements_before_require_dot(parsed_path[i-1])) {
                     reconstructed_path += ".";
                }
                if (el.key_name.find_first_of(".[] ") != std::string::npos) {
                     reconstructed_path += "['" + el.key_name + "']";
                } else {
                    reconstructed_path += el.key_name;
                }
            } else if (el.type == PathElement::Type::INDEX) {
                reconstructed_path += "[" + std::to_string(el.index) + "]";
            }
        }
        if (reconstructed_path.empty() && parsed_path.empty()) return {""};
        if (reconstructed_path.empty() && !parsed_path.empty()) return {};
        return {reconstructed_path};
    }
    throw std::runtime_error("Wildcard expansion is not yet implemented.");
}

std::vector<std::string> PathParser::expand_wildcards(const json& document,
                                                      const std::string& path_str) const {
    if (path_str.empty()) return {""};
    auto parsed = parse(path_str);
    return expand_wildcards(document, parsed);
}


// Static helper implementations
bool PathParser::is_root_path(const std::string& path_str) {
    return trim(path_str) == "$";
}

// This is a heuristic. A path like "a.b" doesn't strictly define 'b' as an array
// until an operation like "a.b[0]" or "a.b.append(...)" is attempted.
// This function primarily checks if the *final* element of a path is an INDEX type,
// or if the context (e.g., an append operation) implies an array.
// The `doc_context` is tricky here. If path_elements_to_target is "foo.bar[0]",
// doc_context would be the root doc. If it's just "foo", doc_context is root.
// A more robust check is usually done at the point of operation by JSONModifier.
// For RedisJSONClient's usage, it was trying to guess if a new path should create an array.
// This simplified version just checks if the last path element hints at an array.
bool PathParser::is_array_path(const std::vector<PathElement>& path_elements_to_target, const json& /*doc_context unused for now*/) {
    if (path_elements_to_target.empty()) {
        return false; // Root path, could be an array, but not by path structure alone. Operation defines it.
    }
    // If the last element is an index, it implies the parent should be an array.
    // If the client code is checking is_array_path for "a.b" *before* appending like "a.b[0]",
    // this function won't help much. It's more for when the path is already "a.b[0]".
    // The usage in RedisJSONClient: `PathParser::is_array_path(parsed_path, current_doc, *_path_parser)`
    // where `parsed_path` is for the *target*.
    // If `parsed_path` ends in an index, or if an operation like `array_append` is used,
    // then it's an array path.
    // For now, let's say a path is an "array path" if its last element implies array access (INDEX)
    // or if it's empty and the operation implies array (e.g. client wants to append to root "$").
    // This is insufficient for the client's predictive need.
    // The `JSONModifier::navigate_to_element` with `create_missing_paths` is smarter.
    // Client should rely on JSONModifier or specific Lua scripts for array creation logic.
    // This function might be misleading as defined.
    // A simpler interpretation: does this path *target* an array element (i.e. ends in an index)?
    const auto& last_el = path_elements_to_target.back();
    return last_el.type == PathElement::Type::INDEX;
    // A more complex version might inspect `doc_context` at `path_elements_to_target` (excluding last element)
    // and see if the element *is* an array. But this is what JSONModifier::get would do.
}

std::string PathParser::escape_key_if_needed(const std::string& key_name) {
    // Simple check: if key contains characters that need bracketing and quotes in JSONPath-like syntax.
    // This isn't full JSONPath spec escaping, just for basic reconstruction.
    if (key_name.empty() || key_name.find_first_of(" .[]\"'") != std::string::npos) {
        // More robust escaping would replace internal quotes, etc.
        std::string escaped_key = key_name;
        // Minimal: replace ' with \' (this is not standard JSONPath, just an example)
        // For nlohmann::json style paths, often no escaping is needed for keys if accessed via .at() or ["key"].
        // For reconstruction into a string path, it matters.
        // Let's assume for now keys are "simple" or reconstruction handles it.
        return "'" + escaped_key + "'"; // Simplified: always quote if contains special chars or is empty
    }
    return key_name;
}

std::string PathParser::reconstruct_path(const std::vector<PathElement>& path_elements) {
    if (path_elements.empty()) {
        return "$"; // Represent root
    }
    std::string p_str;
    bool first_key = true;
    for (size_t i = 0; i < path_elements.size(); ++i) {
        const auto& el = path_elements[i];
        switch (el.type) {
            case PathElement::Type::KEY:
                if (!first_key && (p_str.empty() || p_str.back() != ']')) { // Avoid leading dot if first element, or dot after bracket
                    p_str += ".";
                }
                p_str += el.key_name; // Simplified: assumes key_name is safe or PathParser::escape_key_if_needed was used
                first_key = false;
                break;
            case PathElement::Type::INDEX:
                p_str += "[" + std::to_string(el.index) + "]";
                first_key = false; // After an index, next key doesn't need a dot if it's a quoted key in brackets
                break;
            // TODO: Add other types (SLICE, WILDCARD, etc.)
            default:
                // This should ideally not happen if path_elements is valid
                p_str += ".<ERROR_UNKNOWN_PATH_ELEMENT>";
                break;
        }
    }
    // If path started with an index (e.g. [0].key), p_str would be "[0].key".
    // If it was just keys like "a.b.c", it would be "a.b.c".
    // If it was root and empty elements, we return "$".
    // If path_elements was not empty but resulted in empty string (should not happen with this logic),
    // it would be an issue.
    return p_str;
}


} // namespace redisjson
