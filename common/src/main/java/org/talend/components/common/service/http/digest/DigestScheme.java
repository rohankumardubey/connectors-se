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
package org.talend.components.common.service.http.digest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringTokenizer;

import org.talend.components.common.service.http.common.BasicHeader;
import org.talend.components.common.service.http.common.BasicHeaderValueFormatter;
import org.talend.components.common.service.http.common.BasicHeaderValueParser;
import org.talend.components.common.service.http.common.BasicNameValuePair;
import org.talend.components.common.service.http.common.ByteArrayBuilder;
import org.talend.components.common.service.http.common.CharArrayBuffer;

/**
 * Digest authentication scheme as defined in RFC 2617.
 * Both MD5 (default) and MD5-sess are supported.
 * Currently only qop=auth or no qop is supported. qop=auth-int
 * is unsupported. If auth and auth-int are provided, auth is
 * used. (qop : Quality of Protection)
 * <p>
 * Since the digest username is included as clear text in the generated
 * Authentication header, the charset of the username must be compatible
 * with the HTTP element charset used by the connection.
 * </p>
 *
 * @since 4.0
 */
public class DigestScheme {

    /**
     * Hexa values used when creating 32 character long digest in HTTP DigestScheme
     * in case of authentication.
     *
     * @see #formatHex(byte[])
     */

    private static final char[] HEXADECIMAL =
            { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

    private static final int QOP_UNKNOWN = -1;

    private static final int QOP_MISSING = 0;

    private static final int QOP_AUTH_INT = 1;

    private static final int QOP_AUTH = 2;

    private final Map<String, String> paramMap;

    private ByteArrayBuilder buffer;

    private final Nonce nonce = new Nonce();

    public DigestScheme() {
        this.paramMap = new HashMap<>();
    }

    public String getName() {
        return "digest";
    }

    private static MessageDigest createMessageDigest(final String digAlg) throws UnsupportedDigestAlgorithmException {
        try {
            return MessageDigest.getInstance(digAlg);
        } catch (final Exception e) {
            throw new UnsupportedDigestAlgorithmException(
                    "Unsupported algorithm in HTTP Digest authentication: " + digAlg);
        }
    }

    public String createDigestResponse(final String username, final String password, final BasicHeader authChallenge,
            final DigestAuthContext context) throws AuthenticationException {
        Map<String, BasicNameValuePair> pairs = BasicHeaderValueParser
                .parseParametersAsMap(authChallenge,
                        new BasicHeaderValueParser());

        pairs.entrySet().forEach(k -> this.paramMap.put(k.getKey(), k.getValue().getValue()));

        final String uri = context.getUri();
        final String method = context.getMethod();
        final String realm = Optional
                .ofNullable(pairs.get("realm"))
                .map(BasicNameValuePair::getValue)
                .orElseThrow(() -> new AuthenticationException("No realm value in digest authentication challenge."));

        final String newNonce = Optional
                .ofNullable(pairs.get("nonce"))
                .map(BasicNameValuePair::getValue)
                .orElseThrow(() -> new AuthenticationException("No nonce value in digest authentication challenge."));

        final String opaque = Optional.ofNullable(pairs.get("opaque")).map(BasicNameValuePair::getValue).orElse(null);
        String algorithm = Optional.ofNullable(pairs.get("algorithm")).map(BasicNameValuePair::getValue).orElse("MD5");

        final String qoplist = this.paramMap.get("qop");
        int qop = this.extractQop(qoplist, context.hasPayload());
        if (qop == QOP_UNKNOWN) {
            throw new AuthenticationException("None of the qop methods is supported: " + qoplist);
        }

        final Charset charset = this.extractCharset();

        String digAlg = algorithm;
        if (digAlg.equalsIgnoreCase("MD5-sess")) {
            digAlg = "MD5";
        }

        final MessageDigest digester;
        try {
            digester = createMessageDigest(digAlg);
        } catch (final UnsupportedDigestAlgorithmException ex) {
            throw new AuthenticationException("Unsuppported digest algorithm: " + digAlg);
        }

        final String nc = this.nonce.renew(newNonce);

        if (buffer == null) {
            buffer = new ByteArrayBuilder(128);
        } else {
            buffer.reset();
        }
        buffer.charset(charset);

        // 3.2.2.2: Calculating digest

        buffer.append(username).append(":").append(realm).append(":").append(password);
        if (algorithm.equalsIgnoreCase("MD5-sess")) {
            // H( unq(username-value) ":" unq(realm-value) ":" passwd )
            // ":" unq(nonce-value)
            // ":" unq(cnonce-value)

            // calculated one per session
            final String checksum = formatHex(digester.digest(this.buffer.toByteArray()));
            buffer.reset();
            buffer.append(checksum).append(":").append(newNonce).append(":").append(this.nonce.getCnonce());
        }
        byte[] a1 = buffer.toByteArray();

        final String hasha1 = formatHex(digester.digest(a1));
        buffer.reset();

        buffer.append(method).append(":").append(uri);
        if (qop == QOP_AUTH_INT) {
            final byte[] digest = this.buildEntityDigester(context, digester);
            buffer.append(":").append(formatHex(digest));
        }
        byte[] a2 = buffer.toByteArray();

        final String hasha2 = formatHex(digester.digest(a2));
        buffer.reset();

        // 3.2.2.1
        buffer.append(hasha1).append(":").append(newNonce).append(":");
        if (qop != QOP_MISSING) {
            buffer.append(nc)
                    .append(":")
                    .append(this.nonce.getCnonce())
                    .append(":")
                    .append(qop == QOP_AUTH_INT ? "auth-int" : "auth")
                    .append(":");
        }
        buffer.append(hasha2);
        final byte[] digestInput = buffer.toByteArray();
        buffer.reset();

        final String digest = formatHex(digester.digest(digestInput));

        final CharArrayBuffer digestBuffer = new CharArrayBuffer(128);
        digestBuffer.append("Digest ");

        final List<BasicNameValuePair> params = new ArrayList<>(20);
        params.add(new BasicNameValuePair("username", username));
        params.add(new BasicNameValuePair("realm", realm));
        params.add(new BasicNameValuePair("nonce", newNonce));
        params.add(new BasicNameValuePair("uri", uri));
        params.add(new BasicNameValuePair("response", digest));

        if (qop != QOP_MISSING) {
            params.add(new BasicNameValuePair("qop", qop == QOP_AUTH_INT ? "auth-int" : "auth"));
            params.add(new BasicNameValuePair("nc", nc));
            params.add(new BasicNameValuePair("cnonce", this.nonce.getCnonce()));
        }
        // algorithm cannot be null here
        params.add(new BasicNameValuePair("algorithm", algorithm));
        if (opaque != null) {
            params.add(new BasicNameValuePair("opaque", opaque));
        }

        for (int i = 0; i < params.size(); i++) {
            final BasicNameValuePair param = params.get(i);
            if (i > 0) {
                digestBuffer.append(", ");
            }
            final String name = param.getName();
            final boolean noQuotes = ("nc".equals(name) || "qop".equals(name) || "algorithm".equals(name));
            BasicHeaderValueFormatter.INSTANCE.formatNameValuePair(digestBuffer, param, !noQuotes);
        }
        return digestBuffer.toString();
    }

    private byte[] buildEntityDigester(final DigestAuthContext context,
            final MessageDigest digester) throws AuthenticationException {
        final HttpEntityDigester entityDigester = new HttpEntityDigester(digester);
        try {
            if (context.hasPayload()) {
                writeTo(context.getPayload(), entityDigester);
            }
            entityDigester.close();
            return entityDigester.getDigest();
        } catch (final IOException ex) {
            throw new AuthenticationException("I/O error reading entity content", ex);
        }
    }

    private Charset extractCharset() {
        final String charsetName = this.paramMap.get("charset");
        try {
            return charsetName != null ? Charset.forName(charsetName) : StandardCharsets.ISO_8859_1;
        } catch (final UnsupportedCharsetException ex) {
            return StandardCharsets.ISO_8859_1;
        }
    }

    /**
     * Encodes the 128 bit (16 bytes) MD5 digest into a 32 characters long
     * <CODE>String</CODE> according to RFC 2617.
     *
     * @param binaryData array containing the digest
     * @return encoded MD5, or <CODE>null</CODE> if encoding failed
     */
    static String formatHex(final byte[] binaryData) {
        final int n = binaryData.length;
        final char[] buffer = new char[n * 2];
        for (int i = 0; i < n; i++) {
            final int low = (binaryData[i] & 0x0f);
            final int high = ((binaryData[i] & 0xf0) >> 4);
            buffer[i * 2] = HEXADECIMAL[high];
            buffer[(i * 2) + 1] = HEXADECIMAL[low];
        }

        return new String(buffer);
    }

    /**
     * Extract Quality of Protection from list
     * 
     * @param qopList : list of qop, separate by ','
     * @param contextWithPayload : true if context contains payload.
     * @return qop
     */
    private int extractQop(final String qopList,
            final boolean contextWithPayload) {
        if (qopList == null) {
            return QOP_MISSING;
        }
        int qop = QOP_UNKNOWN;
        final StringTokenizer tok = new StringTokenizer(qopList, ",");
        final Set<String> qopset = new HashSet<>(8);
        while (tok.hasMoreTokens()) {
            final String variant = tok.nextToken().trim();
            qopset.add(variant.toLowerCase(Locale.ROOT));
        }

        if (contextWithPayload && qopset.contains("auth-int")) {
            qop = QOP_AUTH_INT;
        } else if (qopset.contains("auth")) {
            qop = QOP_AUTH;
        } else if (qopset.contains("auth-int")) {
            qop = QOP_AUTH_INT;
        }
        return qop;
    }

    private void writeTo(final byte[] payload, final OutputStream outstream) throws IOException {
        if (outstream == null) {
            throw new IllegalArgumentException("Output stream may not be null");
        }
        InputStream instream = new ByteArrayInputStream(payload);
        try {
            int l;
            byte[] tmp = new byte[2048];
            while ((l = instream.read(tmp)) != -1) {
                outstream.write(tmp, 0, l);
            }
        } finally {
            instream.close();
        }
    }

    @Override
    public String toString() {
        return getName() + this.paramMap.toString();
    }

    public static class AuthenticationException extends Exception {

        public AuthenticationException(final String msg) {
            super(msg);
        }

        public AuthenticationException(final String msg, final Exception e) {
            super(msg, e);
        }
    }

    public static class UnsupportedDigestAlgorithmException extends Exception {

        public UnsupportedDigestAlgorithmException(final String msg) {
            super(msg);
        }

    }

}
