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
            // TODO: WILDCARD, SLICE, FILTER, RECURSIVE_DESCENT will require more complex logic,
            // often returning multiple results or needing a different navigation approach.
            // For now, they'd throw here or be unhandled.
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
                    // If create_missing_paths is true, and current is null, make it an object.
                    if (create_missing_paths && (current->is_null() || i==0 /* allow replacing root with object */) ) {
                        *current = json::object();
                    } else if (!current->is_object()) { // Still not an object after potential creation
                         throw TypeMismatchException(reconstruct_path_string(path_elements, i-1), "object", current->type_name());
                    }
                }
                if (!current->contains(el.key_name)) {
                    if (create_missing_paths) {
                        // Determine if next element is an index to create an array, else object
                        bool next_is_index = (i + 1 < path_elements.size() && path_elements[i+1].type == PathParser::PathElement::Type::INDEX);
                        (*current)[el.key_name] = next_is_index ? json::array() : json::object();
                    } else {
                        throw PathNotFoundException(reconstruct_path_string(path_elements, i));
                    }
                }
                current = &(*current)[el.key_name];
                break;
            case PathParser::PathElement::Type::INDEX:
                 if (!current->is_array()) {
                    if (create_missing_paths && (current->is_null() || i==0)) {
                        *current = json::array();
                    } else if (!current->is_array()){
                        throw TypeMismatchException(reconstruct_path_string(path_elements, i-1), "array", current->type_name());
                    }
                }

                // Negative index handling for arrays being created/resized
                int actual_index = el.index;
                if (el.index < 0) {
                    actual_index = current->size() + el.index;
                }


                if (create_missing_paths) {
                    // Ensure array is large enough, fill with null if necessary
                    // This is for positive indices beyond current size. Negative indices must be valid after adjustment.
                    if (actual_index < 0) throw IndexOutOfBoundsException(el.index, current->size());

                    if (static_cast<size_t>(actual_index) >= current->size()) {
                        // Determine if next element implies an object or array
                        bool next_is_index_for_new_element = (i + 1 < path_elements.size() && path_elements[i+1].type == PathParser::PathElement::Type::INDEX);
                        json new_element_prototype = next_is_index_for_new_element ? json::array() : json::object();

                        // Fill with nulls up to actual_index -1, then place prototype or new value
                        // This simplified version just resizes and sets the target.
                        // nlohmann/json might handle some of this automatically with operator[] if it's an object at next level.
                        // For arrays, direct assignment to an out-of-bounds index is an error.
                        // We need to push_back nulls or the new structure.
                        // current->resize(static_cast<size_t>(actual_index) + 1, json(nullptr)); // This might be too simple if we need to insert specific types
                         while(current->size() <= static_cast<size_t>(actual_index)) {
                            // If this is the *last* element of the path, we'll set the actual value later.
                            // If not, we need to decide: object or array?
                            if (i + 1 < path_elements.size()) {
                                bool next_is_idx = path_elements[i+1].type == PathParser::PathElement::Type::INDEX;
                                current->push_back(next_is_idx ? json::array() : json::object());
                            } else {
                                // This element is the target. It will be overwritten by `set` or used by `del`.
                                // For intermediate creation, push null. `set` will replace it.
                                current->push_back(json(nullptr));
                            }
                        }
                    }
                }
                // Re-check bounds after potential resize/creation
                if (actual_index < 0 || static_cast<size_t>(actual_index) >= current->size()) {
                     throw IndexOutOfBoundsException(el.index, current->size());
                }
                current = &(*current)[actual_index];
                break;
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
    if (path_elements.empty()) {
        throw InvalidPathException("Path cannot be empty for parent navigation.");
    }

    std::vector<PathParser::PathElement> parent_path(path_elements.begin(), path_elements.end() - 1);
    json* parent = navigate_to_element(doc, parent_path, create_missing_paths);

    const auto& last_element = path_elements.back();
    if (last_element.type == PathParser::PathElement::Type::KEY) {
        final_key_or_index = last_element.key_name;
    } else if (last_element.type == PathParser::PathElement::Type::INDEX) {
        if (!parent->is_array() && create_missing_paths && parent->is_null()) {
            *parent = json::array(); // Promote null to array if creating path
        }
        if (!parent->is_array()) {
             throw TypeMismatchException(reconstruct_path_string(path_elements, path_elements.size() - 2), "array", parent->type_name());
        }
        int actual_index = last_element.index;
        if (actual_index < 0) { // adjust negative index
            actual_index = parent->size() + actual_index;
        }
        final_key_or_index = actual_index; // Store the possibly adjusted, non-negative index
    } else {
        throw InvalidPathException("Last path element must be a key or index for parent navigation. Got: " + path_element_to_string(last_element));
    }
    return parent;
}

const json* JSONModifier::navigate_to_parent_const(const json& doc,
                                                   const std::vector<PathParser::PathElement>& path_elements,
                                                   std::variant<std::string, int>& final_key_or_index) const {
    if (path_elements.empty()) {
        throw InvalidPathException("Path cannot be empty for parent navigation.");
    }
    std::vector<PathParser::PathElement> parent_path(path_elements.begin(), path_elements.end() - 1);
    const json* parent = navigate_to_element_const(doc, parent_path);

    const auto& last_element = path_elements.back();
    if (last_element.type == PathParser::PathElement::Type::KEY) {
        final_key_or_index = last_element.key_name;
    } else if (last_element.type == PathParser::PathElement::Type::INDEX) {
        if (!parent->is_array()) {
             throw TypeMismatchException(reconstruct_path_string(path_elements, path_elements.size() - 2), "array", parent->type_name());
        }
        int actual_index = last_element.index;
        if (actual_index < 0) { // adjust negative index
            actual_index = parent->size() + actual_index;
        }
        final_key_or_index = actual_index;
    } else {
        throw InvalidPathException("Last path element must be a key or index. Got: " + path_element_to_string(last_element));
    }
    return parent;
}


// --- Public API Methods ---

json JSONModifier::get(const json& document, const std::vector<PathParser::PathElement>& path_elements) const {
    if (path_elements.empty()) { // Path is root
        return document;
    }
    const json* target_element = navigate_to_element_const(document, path_elements);
    if (!target_element) { // Should be caught by PathNotFoundException inside navigate
        throw PathNotFoundException(reconstruct_path_string(path_elements, path_elements.size()-1));
    }
    return *target_element;
}

void JSONModifier::set(json& document, const std::vector<PathParser::PathElement>& path_elements,
                       const json& value_to_set) {
    if (path_elements.empty()) { // Set root
        document = value_to_set;
        return;
    }

    std::variant<std::string, int> final_accessor;
    json* parent = navigate_to_parent(document, path_elements, final_accessor, true /* create_missing_paths */);

    if (std::holds_alternative<std::string>(final_accessor)) {
        const std::string& key = std::get<std::string>(final_accessor);
        if (!parent->is_object()) { // Should have been made an object by navigate_to_parent if create_missing_paths
             throw TypeMismatchException(reconstruct_path_string(path_elements, path_elements.size()-2), "object", parent->type_name());
        }
        (*parent)[key] = value_to_set;
    } else { // INDEX
        int index = std::get<int>(final_accessor);
         if (!parent->is_array()) { // Should have been made an array
             throw TypeMismatchException(reconstruct_path_string(path_elements, path_elements.size()-2), "array", parent->type_name());
        }
        if (index < 0 ) { // This should have been handled by navigate_to_parent to be positive relative to current size
             throw IndexOutOfBoundsException(index, parent->size()); // Should not happen if navigate_to_parent is correct
        }
        if (static_cast<size_t>(index) == parent->size()) {
            parent->push_back(value_to_set); // Append if index is one past the end
        } else if (static_cast<size_t>(index) < parent->size()) {
            (*parent)[index] = value_to_set; // Overwrite existing element
        } else { // index > parent->size()
            // navigate_to_parent with create_missing_paths should have resized and filled with nulls
            // This case indicates an index still too large after creation logic, or logic error.
            // For safety, throw, but ideally navigate_to_parent handles this.
            // If parent array was just created, it's empty. index must be 0.
            // If it was `key : null` and we are setting `key[0]`, parent becomes `key: []`.
            // Then `parent[0] = value` is valid.
            // This path implies navigate_to_parent didn't extend array sufficiently.
             throw IndexOutOfBoundsException(std::get<int>(final_accessor), parent->size());
        }
    }
}

void JSONModifier::del(json& document, const std::vector<PathParser::PathElement>& path_elements) {
    if (path_elements.empty()) {
        throw InvalidPathException("Cannot delete root document with a path. To clear, set to null or empty object/array.");
    }

    std::variant<std::string, int> final_accessor;
    // Do not create missing paths for delete.
    json* parent = navigate_to_parent(document, path_elements, final_accessor, false /* create_missing_paths=false */);

    if (std::holds_alternative<std::string>(final_accessor)) {
        const std::string& key = std::get<std::string>(final_accessor);
        if (!parent->is_object()) {
            throw TypeMismatchException(reconstruct_path_string(path_elements, path_elements.size()-2), "object", parent->type_name());
        }
        if (!parent->contains(key)) {
            throw PathNotFoundException(reconstruct_path_string(path_elements, path_elements.size()-1));
        }
        parent->erase(key);
    } else { // INDEX
        int index = std::get<int>(final_accessor); // This is already adjusted non-negative index from navigate_to_parent
         if (!parent->is_array()) {
            throw TypeMismatchException(reconstruct_path_string(path_elements, path_elements.size()-2), "array", parent->type_name());
        }
        if (index < 0 || static_cast<size_t>(index) >= parent->size()) {
            // PathNotFound if index is bad, as element doesn't exist at that (adjusted) index
            throw PathNotFoundException(reconstruct_path_string(path_elements, path_elements.size()-1) + " (index " + std::to_string(std::get<int>(final_accessor)) + " out of bounds for size " + std::to_string(parent->size()) + ")");
        }
        parent->erase(static_cast<size_t>(index));
    }
}


bool JSONModifier::exists(const json& document, const std::vector<PathParser::PathElement>& path_elements) const {
    if (path_elements.empty()) {
        return true; // Root always exists
    }
    try {
        navigate_to_element_const(document, path_elements);
        return true;
    } catch (const PathNotFoundException&) {
        return false;
    } catch (const IndexOutOfBoundsException&) {
        return false;
    } catch (const TypeMismatchException&) {
        // If a path segment expects an object but finds an array (or vice-versa),
        // then the full path cannot exist as specified.
        return false;
    }
    // Other exceptions like InvalidPathException should propagate
}

json::value_t JSONModifier::get_type(const json& document,
                                     const std::vector<PathParser::PathElement>& path_elements) const {
    if (path_elements.empty()) {
        return document.type();
    }
    const json* element = navigate_to_element_const(document, path_elements);
    // PathNotFoundException will be thrown by navigate if path is invalid
    return element->type();
}

size_t JSONModifier::get_size(const json& document,
                              const std::vector<PathParser::PathElement>& path_elements) const {
    const json* element = path_elements.empty() ? &document : navigate_to_element_const(document, path_elements);
    // PathNotFoundException will be thrown by navigate if path is invalid

    if (element->is_object() || element->is_array() || element->is_string()) {
        return element->size();
    }
    if (element->is_null()) return 0; // Or throw, depending on desired semantics for size of null
    return 1; // For boolean, number
}


// --- Stubs for Advanced and Array Operations ---
void JSONModifier::merge(json& document, const json& patch, MergeStrategy strategy) {
    // TODO: Implement merge strategies.
    // nlohmann/json's update() method is like a shallow merge for objects (overwrites existing keys).
    // For DEEP merge, custom recursive logic is needed.
    // json::patch() method implements RFC 6902, which is one of the strategies.
    if (strategy == MergeStrategy::PATCH && patch.is_array()) {
         document = document.patch(patch); // Use nlohmann's built-in patch
         return;
    }
    if (strategy == MergeStrategy::OVERWRITE) { // For objects, this is default nlohmann update
        if (document.is_object() && patch.is_object()) {
            document.update(patch);
        } else {
            document = patch; // If types differ or not objects, overwrite wholesale
        }
        return;
    }
    // Other strategies need custom implementation.
    throw std::runtime_error("Merge strategy not yet implemented.");
}

void JSONModifier::apply_patch(json& document, const json& patch_operations) {
    if (!patch_operations.is_array()) {
        throw InvalidPathException("JSON Patch must be an array of operations.");
    }
    document = document.patch(patch_operations); // nlohmann/json built-in support
}

json JSONModifier::diff(const json& old_doc, const json& new_doc) const {
    return json::diff(old_doc, new_doc); // nlohmann/json built-in support
}

void JSONModifier::array_append(json& document, const std::vector<PathParser::PathElement>& path_elements,
                                const json& value_to_append) {
    json* arr_node = navigate_to_element(document, path_elements, true /* create missing, make last element an array */);
    if (!arr_node->is_array()) {
        if (arr_node->is_null() || arr_node->empty() ) { // If it's null or an empty object that was meant to be an array
            *arr_node = json::array(); // Convert to array
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
        if (arr_node->is_null() || arr_node->empty()) {
            *arr_node = json::array();
        } else {
            throw TypeMismatchException(reconstruct_path_string(path_elements, path_elements.size()-1), "array", arr_node->type_name());
        }
    }
    arr_node->insert(arr_node->begin(), value_to_prepend);
}

json JSONModifier::array_pop(json& document, const std::vector<PathParser::PathElement>& path_elements,
                             int index) { // -1 for last element
    json* arr_node = navigate_to_element(document, path_elements, false /* do not create */);
    if (!arr_node->is_array()) {
        throw TypeMismatchException(reconstruct_path_string(path_elements, path_elements.size()-1), "array", arr_node->type_name());
    }
    if (arr_node->empty()) {
        // Changed to use the string-only constructor for this specific message
        throw IndexOutOfBoundsException("Cannot pop from an empty array.");
    }

    int actual_index = index;
    if (index == -1 || index == static_cast<int>(arr_node->size()) -1) { // Pop last
        actual_index = arr_node->size() - 1;
    } else if (index < -1 || index >= static_cast<int>(arr_node->size())) {
         throw IndexOutOfBoundsException(index, arr_node->size());
    }
    // For negative indices other than -1:
    else if (index < 0) {
        actual_index = arr_node->size() + index;
        if (actual_index < 0 || static_cast<size_t>(actual_index) >= arr_node->size()) {
            throw IndexOutOfBoundsException(index, arr_node->size());
        }
    }


    json popped_value = (*arr_node)[actual_index];
    arr_node->erase(actual_index);
    return popped_value;
}

void JSONModifier::array_insert(json& document, const std::vector<PathParser::PathElement>& path_elements,
                                int index, const json& value_to_insert) {
    json* arr_node = navigate_to_element(document, path_elements, true);
     if (!arr_node->is_array()) {
         if (arr_node->is_null() || arr_node->empty()) {
            *arr_node = json::array();
        } else {
            throw TypeMismatchException(reconstruct_path_string(path_elements, path_elements.size()-1), "array", arr_node->type_name());
        }
    }

    int actual_index = index;
    // If index is negative, it counts from the end.
    // If index is larger than current size, nlohmann's insert might place it at the end or throw.
    // Standard behavior for insert usually means if index == size(), it's an append.
    // If index > size(), it's often an error or pads with nulls.
    // For simplicity, let's align with typical vector insert: index must be <= size().
    if (index < 0) {
        actual_index = arr_node->size() + 1 + index; // e.g. -1 means insert before last, so size + (-1) = size-1
                                                 // but insert semantics are "at index", so -1 -> insert at size-1 position
                                                 // if index is -1, actual_index = size. this means append.
                                                 // if index is 0, actual_index = 0.
                                                 // nlohmann json::insert takes an iterator.
                                                 // A simple approach for index:
        if (index == -1 && arr_node->empty()) actual_index = 0; // insert into empty array at pos 0
        else if (index == -1) actual_index = arr_node->size(); // insert at end (append)
        // Other negative indices can be complex for "insert"; often pop/append are clearer.
        // For now, let's restrict general negative indices for insert for simplicity or map them.
        // A common interpretation: insert at arr.begin() + actual_index
        // If index = 0, insert at begin. If index = size(), insert at end (append).
    }


    if (actual_index < 0 || static_cast<size_t>(actual_index) > arr_node->size()) {
        throw IndexOutOfBoundsException(index, arr_node->size());
    }
    arr_node->insert(arr_node->begin() + actual_index, value_to_insert);
}


} // namespace redisjson
