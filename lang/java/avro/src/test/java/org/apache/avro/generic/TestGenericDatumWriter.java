/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.generic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyFloat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.junit.Test;

public class TestGenericDatumWriter {
	public static Encoder mockEncoder1(Encoder encoder, CountDownLatch sizeWrittenSignal, CountDownLatch eltAddedSignal)
			throws IOException {
		CountDownLatch mockFieldVariableSizeWrittenSignal;
		Encoder mockFieldVariableE;
		CountDownLatch mockFieldVariableEltAddedSignal;
		Encoder mockInstance = spy(Encoder.class);
		mockFieldVariableE = encoder;
		mockFieldVariableSizeWrittenSignal = sizeWrittenSignal;
		mockFieldVariableEltAddedSignal = eltAddedSignal;
		doAnswer((stubInvo) -> {
			mockFieldVariableE.writeMapStart();
			mockFieldVariableSizeWrittenSignal.countDown();
			try {
				mockFieldVariableEltAddedSignal.await();
			} catch (InterruptedException e) {
			}
			return null;
		}).when(mockInstance).writeMapStart();
		doAnswer((stubInvo) -> {
			long n = stubInvo.getArgument(0);
			mockFieldVariableE.writeLong(n);
			return null;
		}).when(mockInstance).writeLong(anyLong());
		doAnswer((stubInvo) -> {
			ByteBuffer bytes = stubInvo.getArgument(0);
			mockFieldVariableE.writeBytes(bytes);
			return null;
		}).when(mockInstance).writeBytes(any(ByteBuffer.class));
		doAnswer((stubInvo) -> {
			mockFieldVariableE.writeMapEnd();
			return null;
		}).when(mockInstance).writeMapEnd();
		doAnswer((stubInvo) -> {
			long itemCount = stubInvo.getArgument(0);
			mockFieldVariableE.setItemCount(itemCount);
			return null;
		}).when(mockInstance).setItemCount(anyLong());
		doAnswer((stubInvo) -> {
			boolean b = stubInvo.getArgument(0);
			mockFieldVariableE.writeBoolean(b);
			return null;
		}).when(mockInstance).writeBoolean(anyBoolean());
		doAnswer((stubInvo) -> {
			int en = stubInvo.getArgument(0);
			mockFieldVariableE.writeEnum(en);
			return null;
		}).when(mockInstance).writeEnum(anyInt());
		doAnswer((stubInvo) -> {
			double d = stubInvo.getArgument(0);
			mockFieldVariableE.writeDouble(d);
			return null;
		}).when(mockInstance).writeDouble(anyDouble());
		doAnswer((stubInvo) -> {
			byte[] bytes = stubInvo.getArgument(0);
			int start = stubInvo.getArgument(1);
			int len = stubInvo.getArgument(2);
			mockFieldVariableE.writeFixed(bytes, start, len);
			return null;
		}).when(mockInstance).writeFixed(any(byte[].class), anyInt(), anyInt());
		doAnswer((stubInvo) -> {
			byte[] bytes = stubInvo.getArgument(0);
			int start = stubInvo.getArgument(1);
			int len = stubInvo.getArgument(2);
			mockFieldVariableE.writeBytes(bytes, start, len);
			return null;
		}).when(mockInstance).writeBytes(any(byte[].class), anyInt(), anyInt());
		doAnswer((stubInvo) -> {
			float f = stubInvo.getArgument(0);
			mockFieldVariableE.writeFloat(f);
			return null;
		}).when(mockInstance).writeFloat(anyFloat());
		doAnswer((stubInvo) -> {
			mockFieldVariableE.writeArrayStart();
			mockFieldVariableSizeWrittenSignal.countDown();
			try {
				mockFieldVariableEltAddedSignal.await();
			} catch (InterruptedException e) {
			}
			return null;
		}).when(mockInstance).writeArrayStart();
		doAnswer((stubInvo) -> {
			Utf8 utf8 = stubInvo.getArgument(0);
			mockFieldVariableE.writeString(utf8);
			return null;
		}).when(mockInstance).writeString(any(Utf8.class));
		doAnswer((stubInvo) -> {
			mockFieldVariableE.writeArrayEnd();
			return null;
		}).when(mockInstance).writeArrayEnd();
		doAnswer((stubInvo) -> {
			mockFieldVariableE.writeNull();
			return null;
		}).when(mockInstance).writeNull();
		doAnswer((stubInvo) -> {
			int n = stubInvo.getArgument(0);
			mockFieldVariableE.writeInt(n);
			return null;
		}).when(mockInstance).writeInt(anyInt());
		doAnswer((stubInvo) -> {
			int unionIndex = stubInvo.getArgument(0);
			mockFieldVariableE.writeIndex(unionIndex);
			return null;
		}).when(mockInstance).writeIndex(anyInt());
		doAnswer((stubInvo) -> {
			mockFieldVariableE.startItem();
			return null;
		}).when(mockInstance).startItem();
		doAnswer((stubInvo) -> {
			mockFieldVariableE.flush();
			return null;
		}).when(mockInstance).flush();
		return mockInstance;
	}

	@Test
	public void testWrite() throws IOException {
		String json = "{\"type\": \"record\", \"name\": \"r\", \"fields\": ["
				+ "{ \"name\": \"f1\", \"type\": \"long\" }" + "]}";
		Schema s = new Schema.Parser().parse(json);
		GenericRecord r = new GenericData.Record(s);
		r.put("f1", 100L);
		ByteArrayOutputStream bao = new ByteArrayOutputStream();
		GenericDatumWriter<GenericRecord> w = new GenericDatumWriter<>(s);
		Encoder e = EncoderFactory.get().jsonEncoder(s, bao);
		w.write(r, e);
		e.flush();

		Object o = new GenericDatumReader<GenericRecord>(s).read(null,
				DecoderFactory.get().jsonDecoder(s, new ByteArrayInputStream(bao.toByteArray())));
		assertEquals(r, o);
	}

	@Test
	public void testArrayConcurrentModification() throws Exception, IOException {
		String json = "{\"type\": \"array\", \"items\": \"int\" }";
		Schema s = new Schema.Parser().parse(json);
		final GenericArray<Integer> a = new GenericData.Array<>(1, s);
		ByteArrayOutputStream bao = new ByteArrayOutputStream();
		final GenericDatumWriter<GenericArray<Integer>> w = new GenericDatumWriter<>(s);

		CountDownLatch sizeWrittenSignal = new CountDownLatch(1);
		CountDownLatch eltAddedSignal = new CountDownLatch(1);

		final Encoder e = TestGenericDatumWriter.mockEncoder1(EncoderFactory.get().directBinaryEncoder(bao, null),
				sizeWrittenSignal, eltAddedSignal);

		// call write in another thread
		ExecutorService executor = Executors.newSingleThreadExecutor();
		Future<Void> result = executor.submit(() -> {
			w.write(a, e);
			return null;
		});
		sizeWrittenSignal.await();
		// size has been written so now add an element to the array
		a.add(7);
		// and signal for the element to be written
		eltAddedSignal.countDown();
		try {
			result.get();
			fail("Expected ConcurrentModificationException");
		} catch (ExecutionException ex) {
			assertTrue(ex.getCause() instanceof ConcurrentModificationException);
		}
	}

	@Test
	public void testMapConcurrentModification() throws Exception, IOException {
		String json = "{\"type\": \"map\", \"values\": \"int\" }";
		Schema s = new Schema.Parser().parse(json);
		final Map<String, Integer> m = new HashMap<>();
		ByteArrayOutputStream bao = new ByteArrayOutputStream();
		final GenericDatumWriter<Map<String, Integer>> w = new GenericDatumWriter<>(s);

		CountDownLatch sizeWrittenSignal = new CountDownLatch(1);
		CountDownLatch eltAddedSignal = new CountDownLatch(1);

		final Encoder e = TestGenericDatumWriter.mockEncoder1(EncoderFactory.get().directBinaryEncoder(bao, null),
				sizeWrittenSignal, eltAddedSignal);

		// call write in another thread
		ExecutorService executor = Executors.newSingleThreadExecutor();
		Future<Void> result = executor.submit(() -> {
			w.write(m, e);
			return null;
		});
		sizeWrittenSignal.await();
		// size has been written so now add an entry to the map
		m.put("a", 7);
		// and signal for the entry to be written
		eltAddedSignal.countDown();
		try {
			result.get();
			fail("Expected ConcurrentModificationException");
		} catch (ExecutionException ex) {
			assertTrue(ex.getCause() instanceof ConcurrentModificationException);
		}
	}

	@Test(expected = AvroTypeException.class)
	public void writeDoesNotAllowStringForGenericEnum() throws IOException {
		final String json = "{\"type\": \"record\", \"name\": \"recordWithEnum\"," + "\"fields\": [ "
				+ "{\"name\": \"field\", \"type\": " + "{\"type\": \"enum\", \"name\": \"enum\", \"symbols\": "
				+ "[\"ONE\",\"TWO\",\"THREE\"] " + "}" + "}" + "]}";
		Schema schema = new Schema.Parser().parse(json);
		GenericRecord record = new GenericData.Record(schema);
		record.put("field", "ONE");

		ByteArrayOutputStream bao = new ByteArrayOutputStream();
		GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
		Encoder encoder = EncoderFactory.get().jsonEncoder(schema, bao);

		writer.write(record, encoder);
	}

	private enum AnEnum {
		ONE, TWO, THREE
	};

	@Test(expected = AvroTypeException.class)
	public void writeDoesNotAllowJavaEnumForGenericEnum() throws IOException {
		final String json = "{\"type\": \"record\", \"name\": \"recordWithEnum\"," + "\"fields\": [ "
				+ "{\"name\": \"field\", \"type\": " + "{\"type\": \"enum\", \"name\": \"enum\", \"symbols\": "
				+ "[\"ONE\",\"TWO\",\"THREE\"] " + "}" + "}" + "]}";
		Schema schema = new Schema.Parser().parse(json);
		GenericRecord record = new GenericData.Record(schema);
		record.put("field", AnEnum.ONE);

		ByteArrayOutputStream bao = new ByteArrayOutputStream();
		GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
		Encoder encoder = EncoderFactory.get().jsonEncoder(schema, bao);

		writer.write(record, encoder);
	}

	@Test
	public void writeFieldWithDefaultWithExplicitNullDefaultInSchema() throws Exception {
		Schema schema = schemaWithExplicitNullDefault();
		GenericRecord record = createRecordWithDefaultField(schema);
		writeObject(schema, record);
	}

	@Test
	public void writeFieldWithDefaultWithoutExplicitNullDefaultInSchema() throws Exception {
		Schema schema = schemaWithoutExplicitNullDefault();
		GenericRecord record = createRecordWithDefaultField(schema);
		writeObject(schema, record);
	}

	private Schema schemaWithExplicitNullDefault() {
		String schema = "{\"type\":\"record\",\"name\":\"my_record\",\"namespace\":\"mytest.namespace\",\"doc\":\"doc\","
				+ "\"fields\":[{\"name\":\"f\",\"type\":[\"null\",\"string\"],\"doc\":\"field doc doc\", "
				+ "\"default\":null}]}";
		return new Schema.Parser().parse(schema);
	}

	private Schema schemaWithoutExplicitNullDefault() {
		String schema = "{\"type\":\"record\",\"name\":\"my_record\",\"namespace\":\"mytest.namespace\",\"doc\":\"doc\","
				+ "\"fields\":[{\"name\":\"f\",\"type\":[\"null\",\"string\"],\"doc\":\"field doc doc\"}]}";
		return new Schema.Parser().parse(schema);
	}

	private void writeObject(Schema schema, GenericRecord datum) throws Exception {
		BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(new ByteArrayOutputStream(), null);
		GenericDatumWriter<GenericData.Record> writer = new GenericDatumWriter<>(schema);
		writer.write(schema, datum, encoder);
	}

	private GenericRecord createRecordWithDefaultField(Schema schema) {
		GenericRecord record = new GenericData.Record(schema);
		record.put("f", schema.getField("f").defaultVal());
		return record;
	}
}
