package com.hxm;

import org.objectweb.asm.ClassWriter;

import static jdk.internal.org.objectweb.asm.Opcodes.*;

public class ClassGenerate {
    public static void main(String[] args) throws IllegalAccessException, InstantiationException, NoSuchFieldException {
        ClassWriter cw = new ClassWriter(0);
        cw.visit(V1_5, ACC_PUBLIC + ACC_ABSTRACT + ACC_INTERFACE,
                "com/hxm/Comparable", null, "java/lang/Object",
                new String[] { "com/hxm/Mesurable" });
        cw.visitField(ACC_PUBLIC + ACC_FINAL + ACC_STATIC, "LESS", "I",
                null, new Integer(-1)).visitEnd();
        cw.visitField(ACC_PUBLIC + ACC_FINAL + ACC_STATIC, "EQUAL", "I",
                null, new Integer(0)).visitEnd();
        cw.visitField(ACC_PUBLIC + ACC_FINAL + ACC_STATIC, "GREATER", "I",
                null, new Integer(1)).visitEnd();
        cw.visitMethod(ACC_PUBLIC + ACC_ABSTRACT, "compareTo",
                "(Ljava/lang/Object;)I", null, null).visitEnd();
        cw.visitEnd();
        byte[] b = cw.toByteArray();

        MyClassLoader classLoade=new MyClassLoader();
        Class c=classLoade.defineClass("com.hxm.Comparable",b);
    }
}
