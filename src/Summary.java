import java.io.Serializable;

/**
 * Created by Alec on 4/22/2017.
 * hold data for each daily summary
 */
public class Summary implements Serializable {
	private String ID;
	private int Year;
	private String Month;
	private String Element;
	private int[] Values;
	private char[] MFlags;
	private char[] QFlags;
	private char[] SFlags;

	public Summary() {
		this.Values = new int[31];
		this.MFlags = new char[31];
		this.QFlags = new char[31];
		this.SFlags = new char[31];
	}

	public String toString() {
		String result = "";

		result += ID + " ";
		result += Year + "/";
		result += Month + " ";
		result += Element + ": ";

		for (int i = 0; i < Values.length; i++) {
			result += Values[i] + "-" + MFlags[i] + "-" + QFlags[i] + SFlags[i];
		}

		return result;
	}

	public String getID() {
		return ID;
	}

	public void setID(String ID) {
		this.ID = ID;
	}

	public int getYear() {
		return Year;
	}

	public void setYear(int year) {
		Year = year;
	}

	public String getMonth() {
		return Month;
	}

	public void setMonth(String month) {
		Month = month;
	}

	public String getElement() {
		return Element;
	}

	public void setElement(String element) {
		Element = element;
	}

	public boolean setValue(int day, int value) {
		if (day - 1 > Values.length) {
			return false;
		} else {
			Values[day - 1] = value;
			return true;
		}
	}

	public boolean setMFlag(int day, char value) {
		if (day - 1 > MFlags.length) {
			return false;
		} else {
			MFlags[day - 1] = value;
			return true;
		}
	}

	public boolean setQFlag(int day, char value) {
		if (day - 1 > QFlags.length) {
			return false;
		} else {
			QFlags[day - 1] = value;
			return true;
		}
	}

	public boolean setSFlag(int day, char value) {
		if (day - 1 > SFlags.length) {
			return false;
		} else {
			SFlags[day - 1] = value;
			return true;
		}
	}

	public int[] getValues() {
		return Values;
	}

	public int getValues(int day) {
		if (day - 1 > Values.length) {
			return -9999;
		} else {
			return Values[day - 1];
		}
	}

	public char[] getMFlags() {
		return MFlags;
	}

	public char getMFlags(int day) {
		if (day - 1 > MFlags.length) {
			return Character.MIN_VALUE;
		} else {
			return MFlags[day - 1];
		}
	}

	public char[] getQFlags() {
		return QFlags;
	}

	public char getQFlags(int day) {
		if (day - 1 > QFlags.length) {
			return Character.MIN_VALUE;
		} else {
			return QFlags[day - 1];
		}
	}

	public char[] getSFlags() {
		return SFlags;
	}

	public char getSFlags(int day) {
		if (day - 1 > SFlags.length) {
			return Character.MIN_VALUE;
		} else {
			return SFlags[day - 1];
		}
	}

}
