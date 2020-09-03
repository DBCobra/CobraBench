package kv_interfaces.instrument;

public class LibConstants {
	// Delete operation
	public static long DELETE_WRITE_ID = 0xabcdefabL;
	public static long DELETE_TXN_ID = 0xabcdefabL;
	// key-value exist, but value not encoded (because of the initialization)
	public static long INIT_WRITE_ID = 0xbebeebeeL;
	public static long INIT_TXN_ID = 0xbebeebeeL;
	// read a null value
	public static long NULL_WRITE_ID = 0xdeadbeefL;
	public static long NULL_TXN_ID = 0xdeadbeefL;
	// Only used in WW tracking, when we do blind write.
	public static long MISSING_WRITE_ID = 0xadeafbeeL;
	public static long MISSING_TXN_ID = 0xadeafbeeL;

	public static boolean DEBUG_LIB_FLAG = true;
}