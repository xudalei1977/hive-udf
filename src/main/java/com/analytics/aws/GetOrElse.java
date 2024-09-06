package com.analytics.aws;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;
import org.apache.hadoop.io.BytesWritable;

@Description(name = "getOrElse",
        value = "_FUNC_(str) - get the arguments, if null, return a uuid")
public class GetOrElse extends GenericUDF {

    private transient PrimitiveObjectInspector stringOI = null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("This UDF only takes 1 argument.");
        }

        if (arguments[0].getCategory() != Category.PRIMITIVE ||
                PrimitiveGrouping.STRING_GROUP != PrimitiveObjectInspectorUtils.getPrimitiveGrouping(
                        ((PrimitiveObjectInspector)arguments[0]).getPrimitiveCategory())){
            throw new UDFArgumentTypeException(
                    0, "The argument to GetOrElse() must be a string/varchar");
        }
        stringOI = (PrimitiveObjectInspector) arguments[0];
        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        String value = PrimitiveObjectInspectorUtils.getString(arguments[0].get(), stringOI);
        if (value == null || value.equals("")) {
            return java.util.UUID.randomUUID().toString();
        } else {
            return value;
        }
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "Get Or Else";
    }
}
