package bench.tpcc;

import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import bench.chengTxn.ChengTxnConstants;
import main.Config;
import main.Profiler;

class sortByFirstName implements Comparator<HashMap<String, Object>> {
	@Override
	public int compare(HashMap<String, Object> o1, HashMap<String, Object> o2) {
		String f1 = (String) o1.get("C_FIRST");
		String f2 = (String) o2.get("C_FIRST");
		return f1.compareTo(f2);
	}	
}

public class Utils {
	public static final Random r = new Random(Config.get().SEED);
	private static final char alphanum[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();
	private static final char numeric[] = "0123456789".toCharArray();
	private static final int random_C_C_LAST = r.nextInt(256);
	private static final int random_C_C_ID = r.nextInt(1024);
	private static final int random_C_OL_I_ID = r.nextInt(8192);
	
	public static int RandomNumber(int x, int y) {
		assert x <= y;
		int ret = ThreadLocalRandom.current().nextInt(y-x+1) + x; 
		return ret;
	}
	
	public static int NURand(int A, int x, int y) {
		int C = 0;
		switch(A) {
		case 255: C = random_C_C_LAST; break;
		case 1023: C = random_C_C_ID; break;
		case 8191: C = random_C_OL_I_ID; break;
		default:
			System.out.println("wrong A["+A+"] in NURand");
			assert false;
		}
		return (((RandomNumber(0, A) | RandomNumber(x, y)) + C) % (y-x+1)) + x;
	}
	
	public static String Lastname(int num) {
		String names[] = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};
		String ret = names[(num%1000)/100];
		ret += names[(num/10)%10];
		ret += names[num % 10];
		return ret;
	}
	
	public static String MakeAlphaString(int x, int y) {
		int arrmax = 61;
		int len = RandomNumber(x, y);
		String res = "";
		for(int i = 0; i < len; i++) {
			res += alphanum[RandomNumber(0, arrmax)];
		}
		return res;
	}
	
	public static String MakeNumberString(int x, int y) {
		int arrmax = 9;
		int len = RandomNumber(x, y);
		String res = "";
		for(int i = 0; i < len; i++) {
			res += numeric[RandomNumber(0, arrmax)];
		}
		return res;
	}
	
	public static String[] MakeAddress() {
		String a[] = new String[5];
		a[0] = MakeAlphaString(10, 20);	/* Street 1 */
		a[1] = MakeAlphaString(10, 20); /* Street 2 */
		a[2] = MakeAlphaString(10, 20);	/* City */
		a[3] = MakeAlphaString(2, 2);	/* State */
		a[4] = MakeNumberString(9, 9);	/* Zip */
		return a;
	}
	
	public static String MakeTimeStamp() {
		return new SimpleDateFormat("dd-MM-yyyy").format(new Date());
	}

	public static synchronized void printProgress(int x, int total) {
		int l = 20;
		int num = x * l / total;
		if((x * l) % total != 0) {
			return;
		}
		char a[] = new char[l];
		for(int i = 0; i < l; i++) {
			if(i <= num) {
				a[i] = '*';
			}
			else {
				a[i] = ' ';
			}
		}
		System.out.print("|"+new String(a)+"|\r");
	}
	
	public static void shuffleArray(int a[]) {
		for(int i = 0; i < a.length-1; i++) {
			int j = Utils.RandomNumber(i+1, a.length - 1);
			int tmp = a[i];
			a[i] = a[j];
			a[j] = tmp;
		}
	}
	
	public static void test() {
		for(int i = 0; i < 20; i++) {
			System.out.println(i + ": ");
			System.out.println(Utils.RandomNumber(10, 100));
			System.out.println(Utils.MakeNumberString(10, 30));
			System.out.println(Utils.MakeAlphaString(10, 20));
			System.out.println(Utils.Lastname(Utils.RandomNumber(0, 1000)));
		}
	}

}

