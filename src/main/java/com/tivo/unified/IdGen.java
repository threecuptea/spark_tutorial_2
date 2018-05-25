package com.tivo.unified;

public class IdGen {

    public static long gen(String identifier, int key) {
        long scheme = 0x00;
        long type = (identifier.equals("st")) ? 523 : ((identifier.equals("cl")) ? 313 : 320); // 320 = content, 313 = collection
        //long type = 313;
        long ns = 0x00; // namespace, could be 0x02

        long id = 0;
        id = (scheme << 56);
        id |= type << 40;
        id |= ns << 32;
        id |= key;

        return id;
    }
}
