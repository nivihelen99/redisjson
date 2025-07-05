#include "gtest/gtest.h"
#include "redisjson++/path_parser.h"
#include "redisjson++/exceptions.h"

using namespace redisjson;

TEST(PathParserTest, EmptyPath) {
    PathParser parser;
    auto elements = parser.parse("");
    EXPECT_TRUE(elements.empty());
    EXPECT_TRUE(parser.is_valid_path(""));
}

TEST(PathParserTest, SimpleKey) {
    PathParser parser;
    auto elements = parser.parse("key");
    ASSERT_EQ(elements.size(), 1);
    EXPECT_EQ(elements[0].type, PathParser::PathElement::Type::KEY);
    EXPECT_EQ(elements[0].key_name, "key");
    EXPECT_FALSE(elements[0].is_array_element);
    EXPECT_TRUE(parser.is_valid_path("key"));
}

TEST(PathParserTest, DotSeparatedKeys) {
    PathParser parser;
    auto elements = parser.parse("key1.key2");
    ASSERT_EQ(elements.size(), 2);
    EXPECT_EQ(elements[0].type, PathParser::PathElement::Type::KEY);
    EXPECT_EQ(elements[0].key_name, "key1");
    EXPECT_EQ(elements[1].type, PathParser::PathElement::Type::KEY);
    EXPECT_EQ(elements[1].key_name, "key2");
    EXPECT_TRUE(parser.is_valid_path("key1.key2"));
}

TEST(PathParserTest, SimpleArrayIndex) {
    PathParser parser;
    auto elements = parser.parse("[123]");
    ASSERT_EQ(elements.size(), 1);
    EXPECT_EQ(elements[0].type, PathParser::PathElement::Type::INDEX);
    EXPECT_EQ(elements[0].index, 123);
    EXPECT_TRUE(elements[0].is_array_element);
    EXPECT_TRUE(parser.is_valid_path("[123]"));
}

TEST(PathParserTest, KeyThenArrayIndex) {
    PathParser parser;
    auto elements = parser.parse("object[0]");
    ASSERT_EQ(elements.size(), 2);
    EXPECT_EQ(elements[0].type, PathParser::PathElement::Type::KEY);
    EXPECT_EQ(elements[0].key_name, "object");
    EXPECT_EQ(elements[1].type, PathParser::PathElement::Type::INDEX);
    EXPECT_EQ(elements[1].index, 0);
    EXPECT_TRUE(elements[1].is_array_element);
    EXPECT_TRUE(parser.is_valid_path("object[0]"));
}

TEST(PathParserTest, ArrayIndexThenKey) {
    PathParser parser;
    auto elements = parser.parse("[0].key");
    ASSERT_EQ(elements.size(), 2);
    EXPECT_EQ(elements[0].type, PathParser::PathElement::Type::INDEX);
    EXPECT_EQ(elements[0].index, 0);
    EXPECT_TRUE(elements[0].is_array_element);
    EXPECT_EQ(elements[1].type, PathParser::PathElement::Type::KEY);
    EXPECT_EQ(elements[1].key_name, "key");
    EXPECT_TRUE(parser.is_valid_path("[0].key"));
}

TEST(PathParserTest, QuotedKeyInBrackets) {
    PathParser parser;
    auto elements = parser.parse("['key with spaces']");
    ASSERT_EQ(elements.size(), 1);
    EXPECT_EQ(elements[0].type, PathParser::PathElement::Type::KEY);
    EXPECT_EQ(elements[0].key_name, "key with spaces");
    EXPECT_TRUE(parser.is_valid_path("['key with spaces']"));
}

TEST(PathParserTest, DoubleQuotedKeyInBrackets) {
    PathParser parser;
    auto elements = parser.parse("[\"key.with.dots\"]");
    ASSERT_EQ(elements.size(), 1);
    EXPECT_EQ(elements[0].type, PathParser::PathElement::Type::KEY);
    EXPECT_EQ(elements[0].key_name, "key.with.dots");
    EXPECT_TRUE(parser.is_valid_path("[\"key.with.dots\"]"));
}

TEST(PathParserTest, KeyThenQuotedKeyInBrackets) {
    PathParser parser;
    auto elements = parser.parse("obj['complex key']");
    ASSERT_EQ(elements.size(), 2);
    EXPECT_EQ(elements[0].key_name, "obj");
    EXPECT_EQ(elements[1].type, PathParser::PathElement::Type::KEY);
    EXPECT_EQ(elements[1].key_name, "complex key");
    EXPECT_TRUE(parser.is_valid_path("obj['complex key']"));
}


TEST(PathParserTest, InvalidPaths) {
    PathParser parser;
    EXPECT_FALSE(parser.is_valid_path("."));
    EXPECT_THROW(parser.parse("."), InvalidPathException);

    EXPECT_FALSE(parser.is_valid_path("key."));
    EXPECT_THROW(parser.parse("key."), InvalidPathException);

    EXPECT_FALSE(parser.is_valid_path("key..key2"));
    EXPECT_THROW(parser.parse("key..key2"), InvalidPathException);

    EXPECT_FALSE(parser.is_valid_path("["));
    EXPECT_THROW(parser.parse("["), InvalidPathException);

    EXPECT_FALSE(parser.is_valid_path("[]"));
    EXPECT_THROW(parser.parse("[]"), InvalidPathException);

    EXPECT_FALSE(parser.is_valid_path("['']"));
    EXPECT_THROW(parser.parse("['']"), InvalidPathException);

    EXPECT_FALSE(parser.is_valid_path("[\"\"]"));
    EXPECT_THROW(parser.parse("[\"\"]"), InvalidPathException);

    EXPECT_FALSE(parser.is_valid_path("[abc]")); // Non-integer index without quotes
    EXPECT_THROW(parser.parse("[abc]"), InvalidPathException);

    EXPECT_FALSE(parser.is_valid_path("key[abc]"));
    EXPECT_THROW(parser.parse("key[abc]"), InvalidPathException);

    EXPECT_FALSE(parser.is_valid_path("key.[0]")); // Dot before bracket
    EXPECT_THROW(parser.parse("key.[0]"), InvalidPathException);

    EXPECT_FALSE(parser.is_valid_path("obj.[key]"));
    EXPECT_THROW(parser.parse("obj.[key]"), InvalidPathException);
}

TEST(PathParserTest, NormalizePathValid) {
    PathParser parser;
    // Basic normalization might just return same path if already 'canonical' by its rules
    EXPECT_EQ(parser.normalize_path("key"), "key");
    EXPECT_EQ(parser.normalize_path("key1.key2"), "key1.key2");
    EXPECT_EQ(parser.normalize_path("[0].key"), "[0].key");
    // More advanced: parser.normalize_path("['key']") should be ".key" or "key"
}

TEST(PathParserTest, NormalizePathInvalid) {
    PathParser parser;
    EXPECT_THROW(parser.normalize_path("key..key2"), InvalidPathException);
}

// Add more tests for slice, wildcard, filter, recursive descent when implemented.
// Add tests for expand_wildcards when implemented.

// Basic test for expand_wildcards (current placeholder behavior)
TEST(PathParserTest, ExpandWildcardsNoWildcard) {
    PathParser parser;
    json doc = json::object(); // Dummy document
    std::vector<PathParser::PathElement> parsed_path = parser.parse("key.subkey");
    // Current placeholder throws if wildcards are present, or reconstructs.
    // If it reconstructs:
    auto expanded = parser.expand_wildcards(doc, parsed_path);
    ASSERT_EQ(expanded.size(), 1);
    EXPECT_EQ(expanded[0], "key.subkey");

    auto expanded_str = parser.expand_wildcards(doc, "key.subkey");
     ASSERT_EQ(expanded_str.size(), 1);
    EXPECT_EQ(expanded_str[0], "key.subkey");

}

TEST(PathParserTest, ExpandWildcardsEmptyPath) {
    PathParser parser;
    json doc = json::object();
    auto expanded = parser.expand_wildcards(doc, "");
    ASSERT_EQ(expanded.size(), 1);
    EXPECT_EQ(expanded[0], "");
}

// Future: Test actual wildcard expansion when implemented.
// TEST(PathParserTest, ExpandWildcardsWithActualWildcard) {
//     PathParser parser;
//     json doc = {
//         {"users", {
//             {"user1", {{"name", "Alice"}}},
//             {"user2", {{"name", "Bob"}}}
//         }}
//     };
//     // Assuming "users.*.name" path parsing is implemented
//     // auto expanded = parser.expand_wildcards(doc, "users.*.name");
//     // EXPECT_EQ(expanded.size(), 2);
//     // EXPECT_TRUE(std::find(expanded.begin(), expanded.end(), "users.user1.name") != expanded.end());
//     // EXPECT_TRUE(std::find(expanded.begin(), expanded.end(), "users.user2.name") != expanded.end());
// }

// int main(int argc, char **argv) {
//     ::testing::InitGoogleTest(&argc, argv);
//     return RUN_ALL_TESTS();
// }
