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
            if (!current_segment.empty()) {
                PathElement elem;
                elem.type = PathElement::Type::KEY;
                elem.key_name = current_segment;
                elements.push_back(elem);
                current_segment.clear();
            } else if (elements.empty() && i != 0) {
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
            el.type == PathElement::Type::RECURSIVE_DESCENT ||
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

} // namespace redisjson
