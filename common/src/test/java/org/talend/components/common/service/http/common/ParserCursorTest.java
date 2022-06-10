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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ParserCursorTest {

    @Test
    void parseTo() {
        final String ch = "hi there";
        final ParserCursor cursor = new ParserCursor(0, 10);
        final String result = cursor.parseTo((Character x) -> x == ' ', ch::charAt);
        Assertions.assertEquals("hi", result);
    }

    @Test
    void updatePos() {
        Assertions.assertThrows(IndexOutOfBoundsException.class, () -> new ParserCursor(-5, 10));
        Assertions.assertThrows(IndexOutOfBoundsException.class, () -> new ParserCursor(10, 5));

        final ParserCursor cursor = new ParserCursor(5, 10);
        Assertions.assertThrows(IndexOutOfBoundsException.class, () -> cursor.updatePos(3));
        Assertions.assertThrows(IndexOutOfBoundsException.class, () -> cursor.updatePos(11));
        cursor.updatePos(7);
        Assertions.assertEquals(7, cursor.getPos());
        Assertions.assertFalse(cursor.atEnd());

        cursor.updatePos(10);
        Assertions.assertEquals(10, cursor.getPos());
        Assertions.assertTrue(cursor.atEnd());
    }

    @Test
    void testToString() {
        final ParserCursor cursor = new ParserCursor(5, 10);
        cursor.increment();
        Assertions.assertEquals("[5>6>10]", cursor.toString());
    }
}