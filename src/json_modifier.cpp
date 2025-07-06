#include "redisjson++/json_modifier.h"
#include <algorithm> // For std::find_if if used with filters
#include <variant>   // Added for std::variant related functions like std::holds_alternative, std::get

namespace redisjson {

// Helper function to convert PathElement to a string for error messages
static std::string path_element_to_string(const PathParser::PathElement& el) {
    switch (el.type) {
        case PathParser::PathElement::Type::KEY:
            return "." + el.key_name;
        case PathParser::PathElement::Type::INDEX:
            return "[" + std::to_string(el.index) + "]";
        // TODO: Add other types (SLICE, WILDCARD, etc.) as they are supported
        default:
            return ".<unsupported_path_element>";
    }
}

// Helper to reconstruct path string for errors
static std::string reconstruct_path_string(const std::vector<PathParser::PathElement>& path_elements, size_t up_to_element_idx) {
    std::string p_str;
    if (path_elements.empty() && up_to_element_idx == static_cast<size_t>(-1)) return "$"; // Special case for root

    for (size_t i = 0; i <= up_to_element_idx && i < path_elements.size(); ++i) {
        p_str += path_element_to_string(path_elements[i]);
    }
    if (!p_str.empty() && p_str[0] == '.') return p_str.substr(1); // Remove leading dot
    return p_str;
}


const json* JSONModifier::navigate_to_element_const(const json& doc,
                                                 const std::vector<PathParser::PathElement>& path_elements) const {
    const json* current = &doc;
    for (size_t i = 0; i < path_elements.size(); ++i) {
        const auto& el = path_elements[i];
        if (current == nullptr || current->is_null()) {
             throw PathNotFoundException(reconstruct_path_string(path_elements, i -1));
        }

        switch (el.type) {
            case PathParser::PathElement::Type::KEY:
                if (!current->is_object()) {
                    throw TypeMismatchException(reconstruct_path_string(path_elements, i -1 ), "object", current->type_name());
                }
                if (!current->contains(el.key_name)) {
                    throw PathNotFoundException(reconstruct_path_string(path_elements, i));
                }
                current = &(*current)[el.key_name];
                break;
            case PathParser::PathElement::Type::INDEX:
                if (!current->is_array()) {
                    throw TypeMismatchException(reconstruct_path_string(path_elements, i - 1), "array", current->type_name());
                }
                if (el.index < 0) { // Handle negative indices
                    int actual_index = current->size() + el.index;
                     if (actual_index < 0 || static_cast<size_t>(actual_index) >= current->size()) {
                        throw IndexOutOfBoundsException(el.index, current->size());
                    }
                    current = &(*current)[actual_index];
                } else {
                    if (static_cast<size_t>(el.index) >= current->size()) {
                         throw IndexOutOfBoundsException(el.index, current->size());
                    }
                    current = &(*current)[el.index];
                }
                break;
            default:
                throw InvalidPathException("Unsupported path element type encountered during navigation: " + path_element_to_string(el));
        }
    }
    return current;
}

json* JSONModifier::navigate_to_element(json& doc,
                                        const std::vector<PathParser::PathElement>& path_elements,
                                        bool create_missing_paths) const {
    json* current = &doc;
    for (size_t i = 0; i < path_elements.size(); ++i) {
        const auto& el = path_elements[i];

        switch (el.type) {
            case PathParser::PathElement::Type::KEY:
                if (!current->is_object()) {
                    if (create_missing_paths && (current->is_null() || (i == 0 && current == &doc) ) ) { // Allow replacing root if doc is null initially
                        *current = json::object();
                    } else if (!current->is_object()) {
                         throw TypeMismatchException(reconstruct_path_string(path_elements, i-1), "object", current->type_name());
                    }
                }
                if (!current->contains(el.key_name)) {
                    if (create_missing_paths) {
                        bool next_is_index = (i + 1 < path_elements.size() && path_elements[i+1].type == PathParser::PathElement::Type::INDEX);
                        (*current)[el.key_name] = next_is_index ? json::array() : json::object();
                    } else {
                        throw PathNotFoundException(reconstruct_path_string(path_elements, i));
                    }
                }
                current = &(*current)[el.key_name];
                break;
            case PathParser::PathElement::Type::INDEX:
            {
                 if (!current->is_array()) {
                    if (create_missing_paths && (current->is_null() || (i == 0 && current == &doc) )) {
                        *current = json::array();
                    } else if (!current->is_array()){
                        throw TypeMismatchException(reconstruct_path_string(path_elements, i-1), "array", current->type_name());
                    }
                }

                int actual_index = el.index;
                if (el.index < 0) { // Adjust negative index relative to current size
                    actual_index = current->size() + el.index;
                }

                if (create_missing_paths) {
                    if (actual_index < 0) { // Still negative after adjustment (e.g. large negative index on small array)
                        throw IndexOutOfBoundsException("Index " + std::to_string(el.index) + " out of bounds for array size " + std::to_string(current->size()) + ". Negative index still out of bounds after adjustment during creation.");
                    }
                    if (static_cast<size_t>(actual_index) >= current->size()) {
                         while(current->size() <= static_cast<size_t>(actual_index)) {
                            if (i + 1 < path_elements.size()) { // If not the last element in path, create structure
                                bool next_is_idx_for_new_el = path_elements[i+1].type == PathParser::PathElement::Type::INDEX;
                                current->push_back(next_is_idx_for_new_el ? json::array() : json::object());
                            } else { // Last element, will be set by caller. Push null placeholder.
                                current->push_back(json(nullptr));
                            }
                        }
                    }
                }
                // Re-check bounds after potential resize/creation for non-create_missing_paths or if still invalid
                if (actual_index < 0 || static_cast<size_t>(actual_index) >= current->size()) {
                     throw IndexOutOfBoundsException(el.index, current->size());
                }
                current = &(*current)[actual_index];
                break;
            }
            default:
                throw InvalidPathException("Unsupported path element type: " + path_element_to_string(el));
        }
    }
    return current;
}


json* JSONModifier::navigate_to_parent(json& doc,
                                       const std::vector<PathParser::PathElement>& path_elements,
                                       std::variant<std::string, int>& final_key_or_index,
                                       bool create_missing_paths) const {
    if (path_elements.empty()) { // Should signify root, direct operations on 'doc'
        // This function is for getting a *parent*, so empty path is problematic.
        // Let's throw, as the caller (e.g. `set`) should handle root directly.
        throw InvalidPathException("Path cannot be empty for navigate_to_parent.");
    }

    std::vector<PathParser::PathElement> parent_path_elements(path_elements.begin(), path_elements.end() - 1);
    json* parent_node = navigate_to_element(doc, parent_path_elements, create_missing_paths);

    const auto& last_element = path_elements.back();
    if (last_element.type == PathParser::PathElement::Type::KEY) {
        final_key_or_index = last_element.key_name;
        // Ensure parent is object if creating
        if (create_missing_paths && parent_node && parent_node->is_null()) {
            *parent_node = json::object();
        }
         if (parent_node && !parent_node->is_object()) {
            throw TypeMismatchException(reconstruct_path_string(parent_path_elements, parent_path_elements.empty() ? static_cast<size_t>(-1) : parent_path_elements.size() - 1), "object", parent_node->type_name());
        }
    } else if (last_element.type == PathParser::PathElement::Type::INDEX) {
        // Ensure parent is array if creating
        if (create_missing_paths && parent_node && parent_node->is_null()) {
            *parent_node = json::array();
        }
        if (parent_node && !parent_node->is_array()) {
             throw TypeMismatchException(reconstruct_path_string(parent_path_elements, parent_path_elements.empty() ? static_cast<size_t>(-1) : parent_path_elements.size() - 1), "array", parent_node->type_name());
        }
        int actual_index = last_element.index;
        if (parent_node && actual_index < 0) {
            actual_index = parent_node->size() + actual_index;
        }
        final_key_or_index = actual_index;
    } else {
        throw InvalidPathException("Last path element must be a key or index for parent navigation. Got: " + path_element_to_string(last_element));
    }
    return parent_node;
}

const json* JSONModifier::navigate_to_parent_const(const json& doc,
                                                   const std::vector<PathParser::PathElement>& path_elements,
                                                   std::variant<std::string, int>& final_key_or_index) const {
    if (path_elements.empty()) {
        throw InvalidPathException("Path cannot be empty for navigate_to_parent_const.");
    }
    std::vector<PathParser::PathElement> parent_path_elements(path_elements.begin(), path_elements.end() - 1);
    const json* parent_node = navigate_to_element_const(doc, parent_path_elements);

    const auto& last_element = path_elements.back();
    if (last_element.type == PathParser::PathElement::Type::KEY) {
        final_key_or_index = last_element.key_name;
         if (parent_node && !parent_node->is_object()) {
            throw TypeMismatchException(reconstruct_path_string(parent_path_elements, parent_path_elements.empty() ? static_cast<size_t>(-1) : parent_path_elements.size() - 1), "object", parent_node->type_name());
        }
    } else if (last_element.type == PathParser::PathElement::Type::INDEX) {
         if (parent_node && !parent_node->is_array()) {
             throw TypeMismatchException(reconstruct_path_string(parent_path_elements, parent_path_elements.empty() ? static_cast<size_t>(-1) : parent_path_elements.size() - 1), "array", parent_node->type_name());
        }
        int actual_index = last_element.index;
        if (parent_node && actual_index < 0) {
            actual_index = parent_node->size() + actual_index;
        }
        final_key_or_index = actual_index;
    } else {
        throw InvalidPathException("Last path element must be a key or index. Got: " + path_element_to_string(last_element));
    }
    return parent_node;
}


// --- Public API Methods ---

json JSONModifier::get(const json& document, const std::vector<PathParser::PathElement>& path_elements) const {
    if (path_elements.empty()) {
        return document;
    }
    const json* target_element = navigate_to_element_const(document, path_elements);
    return *target_element; // navigate_to_element_const throws if not found
}

void JSONModifier::set(json& document, const std::vector<PathParser::PathElement>& path_elements,
                       const json& value_to_set, bool create_path, bool overwrite) {
    if (path_elements.empty()) {
        if (overwrite || document.is_null()) {
             document = value_to_set;
        } // If !overwrite and document exists, it's a no-op for root.
        return;
    }

    std::variant<std::string, int> final_accessor;
    json* parent = navigate_to_parent(document, path_elements, final_accessor, create_path);

    // navigate_to_parent would throw if !create_path and parent doesn't exist.
    // If create_path is true, parent should be valid or an error in navigate_to_parent logic.
    if (!parent) {
         // This case should ideally not be reached if navigate_to_parent throws PathNotFound or similar.
         // If create_path was false and path didn't exist, navigate_to_parent would have thrown.
         // If create_path was true, navigate_to_parent should have created the parent.
         // So, getting a null parent here points to an unexpected issue in navigation logic.
         throw std::logic_error("JSONModifier::set - navigate_to_parent returned null unexpectedly. Path: " + reconstruct_path_string(path_elements, path_elements.size()-1));
    }

    if (std::holds_alternative<std::string>(final_accessor)) {
        const std::string& key = std::get<std::string>(final_accessor);
        // Parent type check done in navigate_to_parent if not creating, or parent made object if creating from null.
        if (!parent->is_object()) { // Should not happen if navigate_to_parent is correct
             throw TypeMismatchException(reconstruct_path_string(path_elements, path_elements.size()-2), "object", parent->type_name());
        }
        if (!overwrite && parent->contains(key)) {
            return;
        }
        (*parent)[key] = value_to_set;
    } else { // INDEX
        int index = std::get<int>(final_accessor);
        // Parent type check done in navigate_to_parent.
         if (!parent->is_array()) { // Should not happen
             throw TypeMismatchException(reconstruct_path_string(path_elements, path_elements.size()-2), "array", parent->type_name());
        }

        // The 'index' from final_accessor is already adjusted for negative indices by navigate_to_parent
        // to be the effective, non-negative index for access if the array exists.
        if (index < 0) {
            // This implies original index was too negative even for the current array size,
            // or array was empty and index was negative.
            // navigate_to_parent should have thrown IndexOutOfBounds if it couldn't resolve to a valid effective index.
            // So, this path might indicate an issue or a very specific edge case with empty arrays + negative indices.
            throw IndexOutOfBoundsException("Index " + std::to_string(std::get<int>(final_accessor)) + " out of bounds for array size " + std::to_string(parent->size()) + ". Invalid negative index or internal error at final set stage.");
        }

        if (static_cast<size_t>(index) < parent->size()) { // Index is within current bounds
            if (overwrite) {
                (*parent)[index] = value_to_set;
            } // else: no-op if element exists and not overwriting
        } else if (static_cast<size_t>(index) == parent->size()) { // Append
            if (create_path) {
                parent->push_back(value_to_set);
            } else {
                 throw IndexOutOfBoundsException("Index " + std::to_string(std::get<int>(final_accessor)) + " out of bounds for array size " + std::to_string(parent->size()) + ". Cannot append to array, create_path is false.");
            }
        } else { // index > parent->size(), attempting to set out of bounds
            if (create_path) {
                while (parent->size() < static_cast<size_t>(index)) {
                    parent->push_back(json(nullptr));
                }
                parent->push_back(value_to_set);
            } else {
                throw IndexOutOfBoundsException(std::get<int>(final_accessor), parent->size());
            }
        }
    }
}

void JSONModifier::del(json& document, const std::vector<PathParser::PathElement>& path_elements) {
    if (path_elements.empty()) {
        throw InvalidPathException("Cannot delete root document with a path. To clear, set to null or empty object/array.");
    }

    std::variant<std::string, int> final_accessor;
    json* parent = navigate_to_parent(document, path_elements, final_accessor, false /* create_missing_paths=false for del */);

    if (!parent) { // Should be caught by navigate_to_parent throwing PathNotFoundException
         throw PathNotFoundException(reconstruct_path_string(path_elements, path_elements.size()-1), "Parent path for delete operation not found.");
    }

    if (std::holds_alternative<std::string>(final_accessor)) {
        const std::string& key = std::get<std::string>(final_accessor);
        if (!parent->is_object()) { // Should be caught by navigate_to_parent
            throw TypeMismatchException(reconstruct_path_string(path_elements, path_elements.size()-2), "object", parent->type_name());
        }
        if (!parent->contains(key)) {
            throw PathNotFoundException(reconstruct_path_string(path_elements, path_elements.size()-1));
        }
        parent->erase(key);
    } else { // INDEX
        int index = std::get<int>(final_accessor);
         if (!parent->is_array()) { // Should be caught by navigate_to_parent
            throw TypeMismatchException(reconstruct_path_string(path_elements, path_elements.size()-2), "array", parent->type_name());
        }
        // final_accessor index from navigate_to_parent is already adjusted for negative indices
        if (index < 0 || static_cast<size_t>(index) >= parent->size()) {
            throw PathNotFoundException(reconstruct_path_string(path_elements, path_elements.size()-1) + " (index " + std::to_string(std::get<int>(final_accessor)) + " out of bounds for size " + std::to_string(parent->size()) + ")");
        }
        parent->erase(static_cast<size_t>(index));
    }
}


bool JSONModifier::exists(const json& document, const std::vector<PathParser::PathElement>& path_elements) const {
    if (path_elements.empty()) {
        return !document.is_null(); // Root exists if document is not null
    }
    try {
        // navigate_to_element_const will throw if path doesn't resolve
        // to an existing element.
        navigate_to_element_const(document, path_elements);
        return true;
    } catch (const PathNotFoundException&) {
        return false;
    } catch (const IndexOutOfBoundsException&) {
        return false;
    } catch (const TypeMismatchException&) {
        return false;
    }
}

json::value_t JSONModifier::get_type(const json& document,
                                     const std::vector<PathParser::PathElement>& path_elements) const {
    if (path_elements.empty()) {
        return document.type();
    }
    const json* element = navigate_to_element_const(document, path_elements);
    return element->type();
}

size_t JSONModifier::get_size(const json& document,
                              const std::vector<PathParser::PathElement>& path_elements) const {
    const json* element = path_elements.empty() ? &document : navigate_to_element_const(document, path_elements);

    switch (element->type()) {
        case json::value_t::object:
        case json::value_t::array:
        case json::value_t::string:
            return element->size();
        case json::value_t::null:
            return 0;
        case json::value_t::number_integer:
        case json::value_t::number_unsigned:
        case json::value_t::number_float:
        case json::value_t::boolean:
            return 1;
        case json::value_t::binary:
            return element->size();
        case json::value_t::discarded:
            throw std::runtime_error("Cannot get size of a discarded JSON element at path: " + reconstruct_path_string(path_elements, path_elements.empty() ? static_cast<size_t>(-1) : path_elements.size() -1));
        default:
            throw std::runtime_error("Unknown JSON element type encountered in get_size at path: " + reconstruct_path_string(path_elements, path_elements.empty() ? static_cast<size_t>(-1) : path_elements.size() -1));
    }
}


// --- Stubs for Advanced and Array Operations ---
void JSONModifier::merge(json& document, const json& patch, MergeStrategy strategy) {
    if (strategy == MergeStrategy::PATCH && patch.is_array()) {
         document = document.patch(patch);
         return;
    }
    if (strategy == MergeStrategy::OVERWRITE) {
        if (document.is_object() && patch.is_object()) {
            document.update(patch); // nlohmann's update is overwrite for objects
        } else { // If types differ or not both objects, or one isn't object, wholesale replace
            document = patch;
        }
        return;
    }
    // TODO: Implement DEEP, SHALLOW (distinct from OVERWRITE if patch is not object), APPEND
    throw std::runtime_error("Merge strategy not fully implemented yet.");
}

void JSONModifier::apply_patch(json& document, const json& patch_operations) {
    if (!patch_operations.is_array()) {
        throw InvalidArgumentException("JSON Patch must be an array of operations.");
    }
    try {
        document = document.patch(patch_operations);
    } catch (const json::exception& e) { // Catch nlohmann::json specific exceptions
        throw PatchFailedException(std::string("JSON Patch application failed: ") + e.what());
    }
}

json JSONModifier::diff(const json& old_doc, const json& new_doc) const {
    return json::diff(old_doc, new_doc);
}

void JSONModifier::array_append(json& document, const std::vector<PathParser::PathElement>& path_elements,
                                const json& value_to_append) {
    json* arr_node = navigate_to_element(document, path_elements, true);
    if (!arr_node->is_array()) {
        if (arr_node->is_null() || (arr_node->is_object() && arr_node->empty()) ) {
            *arr_node = json::array();
        } else {
            throw TypeMismatchException(reconstruct_path_string(path_elements, path_elements.size()-1), "array", arr_node->type_name());
        }
    }
    arr_node->push_back(value_to_append);
}

void JSONModifier::array_prepend(json& document, const std::vector<PathParser::PathElement>& path_elements,
                                 const json& value_to_prepend) {
    json* arr_node = navigate_to_element(document, path_elements, true);
     if (!arr_node->is_array()) {
        if (arr_node->is_null() || (arr_node->is_object() && arr_node->empty())) {
            *arr_node = json::array();
        } else {
            throw TypeMismatchException(reconstruct_path_string(path_elements, path_elements.size()-1), "array", arr_node->type_name());
        }
    }
    arr_node->insert(arr_node->begin(), value_to_prepend);
}

json JSONModifier::array_pop(json& document, const std::vector<PathParser::PathElement>& path_elements,
                             int index) {
    json* arr_node = navigate_to_element(document, path_elements, false );
    if (!arr_node->is_array()) {
        throw TypeMismatchException(reconstruct_path_string(path_elements, path_elements.size()-1), "array", arr_node->type_name());
    }
    if (arr_node->empty()) {
        throw IndexOutOfBoundsException("Cannot pop from an empty array.");
    }

    int actual_index = index;
    if (index == -1 ) {
        actual_index = arr_node->size() - 1;
    } else if (index < 0) { // Other negative indices
        actual_index = arr_node->size() + index;
    }
    // Validate actual_index
    if (actual_index < 0 || static_cast<size_t>(actual_index) >= arr_node->size()) {
         throw IndexOutOfBoundsException(index, arr_node->size());
    }

    json popped_value = (*arr_node)[actual_index];
    arr_node->erase(actual_index);
    return popped_value;
}

void JSONModifier::array_insert(json& document, const std::vector<PathParser::PathElement>& path_elements,
                                int index, const json& value_to_insert) {
    json* arr_node = navigate_to_element(document, path_elements, true);
     if (!arr_node->is_array()) {
         if (arr_node->is_null() || (arr_node->is_object() && arr_node->empty())) {
            *arr_node = json::array();
        } else {
            throw TypeMismatchException(reconstruct_path_string(path_elements, path_elements.size()-1), "array", arr_node->type_name());
        }
    }

    int actual_index = index;
    if (index < 0) { // Negative index means from the end. -1 means insert at the end (append like).
                     // nlohmann's insert takes iterator: begin() + offset.
                     // If actual_index is to be "before" that position, counting from end.
        actual_index = arr_node->size() + index + 1;
        // e.g. size=3, index=-1 (insert before last element) -> actual_index = 3 - 1 + 1 = 3. (insert at end / append)
        // This semantic for negative index in insert might be non-standard.
        // Python's list.insert(-1, x) inserts before the last element.
        // To match Python's list.insert(idx, val): if idx < 0, idx = max(0, size + idx).
        // Let's use simpler: if index is negative, map to positive from start, or throw for now.
        // For insert, usually positive indices or `begin()`/`end()` iterators are clearer.
        // A common interpretation: if index is -1, it means insert at size (append).
        // If index is 0, insert at beginning.
        // Let's assume a simple positive index mapping or specific values:
        if (index == -1) { // Special case: -1 often means "at the end" for insertion
            actual_index = arr_node->size();
        } else if (index < -1) { // Other negative indices are complex for insert.
             throw IndexOutOfBoundsException("Index " + std::to_string(index) + " out of bounds for array size " + std::to_string(arr_node->size()) + ". General negative indices for insert not supported this way, use 0 or -1 (for end).");
        }
        // if index >=0, it's used as is.
    }


    if (actual_index < 0 || static_cast<size_t>(actual_index) > arr_node->size()) {
        throw IndexOutOfBoundsException(index, arr_node->size());
    }
    arr_node->insert(arr_node->begin() + actual_index, value_to_insert);
}


} // namespace redisjson
