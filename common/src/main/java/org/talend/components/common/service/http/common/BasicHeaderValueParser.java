/*
 * Copyright (C) 2006-2022 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.common.service.http.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import lombok.RequiredArgsConstructor;

/**
 * Basic implementation for parsing header values into elements.
 * Instances of this class are stateless and thread-safe.
 * Derived classes are expected to maintain these properties.
 *
 * @since 4.0
 */
public class BasicHeaderValueParser {

    /**
     * A default instance of this class, for use as default or fallback.
     * Note that {@link BasicHeaderValueParser} is not a singleton, there
     * can be many instances of the class itself and of derived classes.
     * The instance here provides non-customized, default behavior.
     */
    public static final BasicHeaderValueParser DEFAULT = new BasicHeaderValueParser();

    private static final char PARAM_DELIMITER = ';';

    private static final char ELEM_DELIMITER = ',';

    private static final char[] ALL_DELIMITERS = new char[] { PARAM_DELIMITER, ELEM_DELIMITER };

    private static final String BUFFER_MAY_NOT_BE_NULL = "Char array buffer may not be null";

    private static final String CURSOR_MAY_NOT_BE_NULL = "Parser cursor may not be null";

    /**
     * Parses elements with the given parser.
     *
     * @param value the header value to parse
     * @param parser the parser to use, or <code>null</code> for default
     * @return array holding the header elements, never <code>null</code>
     */
    public static final BasicHeaderElement[] parseElements(final String value, BasicHeaderValueParser parser)
            throws ParseException {

        if (value == null) {
            throw new IllegalArgumentException("Value to parse may not be null");
        }

        if (parser == null)
            parser = BasicHeaderValueParser.DEFAULT;

        CharArrayBuffer buffer = new CharArrayBuffer(value.length());
        buffer.append(value);
        ParserCursor cursor = new ParserCursor(0, value.length());
        return parser.parseElements(buffer, cursor);
    }

    // non-javadoc, see interface HeaderValueParser
    public BasicHeaderElement[] parseElements(final CharArrayBuffer buffer, final ParserCursor cursor) {

        if (buffer == null) {
            throw new IllegalArgumentException(BUFFER_MAY_NOT_BE_NULL);
        }
        if (cursor == null) {
            throw new IllegalArgumentException(CURSOR_MAY_NOT_BE_NULL);
        }

        List<BasicHeaderElement> elements = new ArrayList<>();
        while (!cursor.atEnd()) {
            BasicHeaderElement element = parseHeaderElement(buffer, cursor);
            if (!(element.getName().length() == 0 && element.getValue() == null)) {
                elements.add(element);
            }
        }
        return elements.toArray(new BasicHeaderElement[elements.size()]);
    }

    // non-javadoc, see interface HeaderValueParser
    public BasicHeaderElement parseHeaderElement(final CharArrayBuffer buffer, final ParserCursor cursor) {

        if (buffer == null) {
            throw new IllegalArgumentException(BUFFER_MAY_NOT_BE_NULL);
        }
        if (cursor == null) {
            throw new IllegalArgumentException(CURSOR_MAY_NOT_BE_NULL);
        }

        BasicNameValuePair nvp = parseNameValuePair(buffer, cursor);
        BasicNameValuePair[] params = null;
        if (!cursor.atEnd()) {
            char ch = buffer.charAt(cursor.getPos() - 1);
            if (ch != ELEM_DELIMITER) {
                params = parseParameters(buffer, cursor);
            }
        }
        return createHeaderElement(nvp.getName(), nvp.getValue(), params);
    }

    /**
     * Creates a header element.
     * Called from {@link #parseHeaderElement}.
     *
     * @return a header element representing the argument
     */
    protected BasicHeaderElement createHeaderElement(final String name, final String value,
            final BasicNameValuePair[] params) {
        return new BasicHeaderElement(name, value, params);
    }

    public static Map<String, BasicNameValuePair> parseParametersAsMap(final BasicHeader header,
            final BasicHeaderValueParser parser) {
        BasicNameValuePair[] pairs = parseParameters(header, parser);
        Map<String, BasicNameValuePair> map = new HashMap<>();
        for (BasicNameValuePair p : pairs) {
            map.put(p.getName(), p);
        }

        return map;
    }

    public static BasicNameValuePair[] parseParameters(final BasicHeader header, final BasicHeaderValueParser parser) {

        BasicNameValuePair wwwAuthenticateType = null;
        String value = header.getValue().trim();

        boolean isWWWAuthent = "WWW-Authenticate".equals(header.getName());
        Optional<String> type = Optional.ofNullable(extractType(value));

        if (isWWWAuthent && type.isPresent()) {
            value = value.substring(type.get().length());
        }

        BasicNameValuePair[] nameValuePairs = parseParameters(value, parser);

        if (isWWWAuthent && type.isPresent()) {
            wwwAuthenticateType = new BasicNameValuePair("WWW-Authenticate-Type", type.get());
            nameValuePairs = Arrays.copyOf(nameValuePairs, nameValuePairs.length + 1);
            nameValuePairs[nameValuePairs.length - 1] = wwwAuthenticateType;
        }

        return nameValuePairs;
    }

    private static String extractType(String challenge) {
        char[] cc = challenge.trim().toCharArray();
        boolean space = false;
        boolean equal = false;
        StringBuilder type = new StringBuilder();
        for (char c : cc) {
            if (c == ' ') {
                space = true;
                break;
            }

            if (c == '=') {
                equal = true;
            }
            type.append(c);
        }

        if (space && !equal) {
            return type.toString();
        }

        return null;
    }

    /**
     * Parses parameters with the given parser.
     *
     * @param value the parameter list to parse
     * @param parser the parser to use, or <code>null</code> for default
     * @return array holding the parameters, never <code>null</code>
     */
    public static final BasicNameValuePair[] parseParameters(final String value, BasicHeaderValueParser parser)
            throws ParseException {

        if (value == null) {
            throw new IllegalArgumentException("Value to parse may not be null");
        }

        if (parser == null)
            parser = BasicHeaderValueParser.DEFAULT;

        CharArrayBuffer buffer = new CharArrayBuffer(value.length());
        buffer.append(value);
        ParserCursor cursor = new ParserCursor(0, value.length());
        return parser.parseParameters(buffer, cursor);
    }

    // non-javadoc, see interface HeaderValueParser
    public BasicNameValuePair[] parseParameters(final CharArrayBuffer buffer, final ParserCursor cursor) {

        if (buffer == null) {
            throw new IllegalArgumentException(BUFFER_MAY_NOT_BE_NULL);
        }
        if (cursor == null) {
            throw new IllegalArgumentException(CURSOR_MAY_NOT_BE_NULL);
        }

        int pos = cursor.getPos();
        int indexTo = cursor.getUpperBound();

        while (pos < indexTo) {
            char ch = buffer.charAt(pos);
            if (CharArrayBuffer.isWhitespace(ch)) {
                pos++;
            } else {
                break;
            }
        }
        cursor.updatePos(pos);
        if (cursor.atEnd()) {
            return new BasicNameValuePair[] {};
        }

        List<BasicNameValuePair> params = new ArrayList<>();
        while (!cursor.atEnd()) {
            BasicNameValuePair param = parseNameValuePair(buffer, cursor);
            params.add(param);
        }

        return params.toArray(new BasicNameValuePair[params.size()]);
    }

    // non-javadoc, see interface HeaderValueParser
    public BasicNameValuePair parseNameValuePair(final CharArrayBuffer buffer, final ParserCursor cursor) {
        return parseNameValuePair(buffer, cursor, ALL_DELIMITERS);
    }

    private static boolean isOneOf(final char ch, final char[] chs) {
        if (chs != null) {
            for (int i = 0; i < chs.length; i++) {
                if (ch == chs[i]) {
                    return true;
                }
            }
        }
        return false;
    }

    private BasicNameValuePair parseNameValuePair(final CharArrayBuffer buffer, final ParserCursor cursor,
            final char[] delimiters) {

        if (buffer == null) {
            throw new IllegalArgumentException(BUFFER_MAY_NOT_BE_NULL);
        }
        if (cursor == null) {
            throw new IllegalArgumentException(CURSOR_MAY_NOT_BE_NULL);
        }

        final String brutName = cursor.parseTo((Character x) -> x == '=' || isOneOf(x, delimiters), buffer::charAt);
        final String name = brutName.trim();

        if (cursor.atEnd()) {
            return createNameValuePair(name, null);
        }
        char sepChar = buffer.charAt(cursor.getPos());
        if (isOneOf(sepChar, delimiters)) {
            cursor.increment();
            return createNameValuePair(name, null);
        }
        if (sepChar == '=') {
            cursor.increment();
        }

        // Find value
        final Predicate<Character> valueEnd = new ValueParser(delimiters);
        String brutValue = cursor.parseTo(valueEnd, buffer::charAt);
        String value = brutValue.trim();
        if (value.charAt(0) == '"' && value.length() > 1 && value.charAt(value.length() - 1) == '"') {
            value = value.substring(1, value.length() - 1);
        }
        cursor.increment();

        return createNameValuePair(name, value);
    }

    @RequiredArgsConstructor
    private static class ValueParser implements Predicate<Character> {

        private final char[] delimiters;

        private boolean quoted = false;

        private boolean escaped = false;

        @Override
        public boolean test(Character ch) {
            if (ch == '"' && !escaped) {
                quoted = !quoted;
            }
            if (!quoted && !escaped && isOneOf(ch, delimiters)) {
                return true;
            }
            if (escaped) {
                escaped = false;
            } else {
                escaped = quoted && ch == '\\';
            }
            return false;
        }
    }

    /**
     * Creates a name-value pair.
     * Called from {@link #parseNameValuePair}.
     *
     * @param name the name
     * @param value the value, or <code>null</code>
     * @return a name-value pair representing the arguments
     */
    protected BasicNameValuePair createNameValuePair(final String name, final String value) {
        return new BasicNameValuePair(name, value);
    }

    public static final class ParseException extends RuntimeException {

    }

}
