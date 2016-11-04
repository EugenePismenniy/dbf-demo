package ua.home.dbf;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.xBaseJ.DBF;
import org.xBaseJ.fields.DateField;
import org.xBaseJ.fields.Field;
import org.xBaseJ.fields.FloatField;
import org.xBaseJ.fields.LogicalField;
import org.xBaseJ.xBaseJException;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Calendar;

/**
 * Hello DBF!
 *
 */
public class App 
{

	public static final String DBF_ENCODE = "cp866";

	public static void main(String[] args) throws IOException {

		Path path = Paths.get(System.getProperty("user.home"));

		String replacement = File.separator + File.separator;

		Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {

				String fileName = file.toString().replace(File.separator, replacement);

				if (!fileName.contains("//.")  && StringUtils.endsWithIgnoreCase(fileName, ".dbf")) {

					System.out.println(fileName);

					try {
						dbfProcessing(fileName);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

				return FileVisitResult.CONTINUE;
			}
		});


	}


	public static void dbfProcessing(String dbfName) throws IOException, xBaseJException {


		DBF dbf = new DBF(dbfName, DBF_ENCODE);
		try {
			final int recordCount = dbf.getRecordCount();
			final int fieldCount = dbf.getFieldCount();

//			System.out.printf("Record Count = %s\n", recordCount);
//			System.out.printf("Field Count = %s\n", fieldCount);

			//StringBuilder recordLineFormatBuilder = new StringBuilder();

			for (int fieldIndex = 1; fieldIndex <= fieldCount; fieldIndex ++) {

				Field field = dbf.getField(fieldIndex);

				String fieldName = field.Name;
				char fieldType = field.getType();
				int length = field.Length;
				String simpleFieldClassName = field.getClass().getSimpleName();

				int decimalPositionCount = field.getDecimalPositionCount();

//				System.out.printf("Field Name = '%s'; Type = '%s'; Length = %s; Simple Field Class Name = '%s'; Decimal Position Count = %s\n"
//						, fieldName, fieldType, length, simpleFieldClassName, decimalPositionCount);

//				recordLineFormatBuilder.append('%').append(fieldIndex).append('$');
//				switch (fieldType) {
//
//					case 'D': {
//						recordLineFormatBuilder.append('-').append(10).append("tF");
//					} break;
//
//
//					case 'C': {
//						recordLineFormatBuilder.append('-').append(length).append('s');
//					} break;
//
//					case 'N': {
//						recordLineFormatBuilder.append(decimalPositionCount > 0 ? length + decimalPositionCount + 1 : length).append('s');
//					} break;
//
//					default: {
//						throw new IllegalArgumentException("Unknown field(" + fieldIndex + ") fieldType = '" + fieldType + "'");
//					}
//
//				}
//				recordLineFormatBuilder.append(' ');


			}
		//	recordLineFormatBuilder.append('\n');

			// -------------------------------------------------------------------------------

//			final String recordLineFormat = recordLineFormatBuilder.toString();


			Object[] values = new Object[fieldCount];

			for (int record = 1; record <= recordCount; record ++) {

				dbf.read();

				for (int fieldIndex = 1, i = 0; fieldIndex <= fieldCount; fieldIndex ++, i ++) {

					Field field = dbf.getField(fieldIndex);
					char type = field.getType();

					String value = StringUtils.trimToNull(field.get());

					if (value == null) {
						values[i] = null;
					} else {

						switch (type) {

							case 'D': {
								DateField dateField = (DateField) field;
								Calendar calendar = dateField.getCalendar();
								values[i] = calendar.getTime();
							}
							break;


							case 'N':
							case 'F':  {
								if (!".".equals(value)) {
									BigDecimal bigDecimal = new BigDecimal(value);

									if (field.getDecimalPositionCount() == 0) {
										values[i] = bigDecimal.toBigInteger();
									} else {
										values[i] = bigDecimal;
									}

									//System.out.printf("%s;  len = %s; dec = %s; origin value = '%s'\n", values[i], field.Length, field.getDecimalPositionCount(), value);
								} else {
									values[i] = null;
								}
							}
							break;

							case 'L': {
								LogicalField logicalField = (LogicalField) field;
								values[i] = logicalField.getBoolean();
							}  break;


							default: {
								values[i] = value;
							}

						}
					}
				}

	//			System.out.printf(recordLineFormat, values);
			}
		} finally {
			closeDbf(dbf);
		}
	}

	public static void closeDbf(DBF dbf) {
		if (dbf != null) {
			try {
				dbf.close();
			} catch (Exception e) {
				System.out.printf("Error close DBF: %s, %s\n",
						ExceptionUtils.getMessage(e), ExceptionUtils.getMessage(e.getCause()));
			}
		}
	}
}
