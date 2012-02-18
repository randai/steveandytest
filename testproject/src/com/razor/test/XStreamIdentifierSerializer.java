package com.razor.test;
import static com.esotericsoftware.minlog.Log.*;

import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serialize.StringSerializer;
import com.financialogix.xstream.common.data.XStreamIdentifier;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

/**
 * Writes/Reads a {@link BigDecimal} to/from the {@link ByteBuffer}
 * 
 * @author Joe Jensen (joe.m.jensen@gmail.com)
 */
public class XStreamIdentifierSerializer extends Serializer
{
    public XStreamIdentifier readObjectData(ByteBuffer buffer, Class type)
    {
        String s = StringSerializer.get(buffer);
        return new XStreamIdentifier( s );
    }

    public void writeObjectData(ByteBuffer buffer, Object object)
    {
    	XStreamIdentifier d = (XStreamIdentifier) object;
        String s = d.toString();
        StringSerializer.put(buffer, s);
    }
}
