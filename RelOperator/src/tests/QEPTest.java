
package tests;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import global.AttrOperator;
import global.AttrType;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import relop.FileScan;
import relop.HashJoin;
import relop.IndexScan;
import relop.Predicate;
import relop.Projection;
import relop.Schema;
import relop.Selection;
import relop.Tuple;

// YOUR CODE FOR PART3 SHOULD GO HERE.

public class QEPTest extends TestDriver {
	
	private static final String TEST_NAME = "Query Evaluation Plan tests";

	private static final String EMPLOYEE_FILE_NAME = "Employee.txt";
	
	private static final String DEPARTMENT_FILE_NAME = "Department.txt";
	
	private static String folderLocation;
	
	/** Employee table schema */
	private static Schema s_employee;
	
	/** Department table schema */
	private static Schema s_department;
	
	private static HeapFile employee;
	
	private static HeapFile department;
	
	private static HashIndex ixdeptId;
	
	/**
	 * Test Application entry point; run All tests.
	 */
	
	public static void main(String argv[]){
		
		if (argv.length > 0){
			System.out.println(argv[0]);
			folderLocation = argv[0];
			System.out.println("Fetching files Employee.txt and Department.txt from location : " + folderLocation);
		}
		else {
			folderLocation = new File("").getAbsolutePath()+"/src/tests/SampleData/";
			System.out.println("Fetching files Employee.txt and Department.txt from default Location : " + folderLocation);
		}
		
		QEPTest qep = new QEPTest();
		qep.create_minibase();
		
		//initialize schema for Employee and Department
		/**
		 * Employee (EmpId, Name, Age, Salary, DeptID), and
		   Department (DeptId, Name, MinSalary, MaxSalary)
		 */
		s_employee = new Schema(5);
		s_employee.initField(0, AttrType.INTEGER, 4, "EmpId");
		s_employee.initField(1, AttrType.STRING, 50, "Name");
		s_employee.initField(2, AttrType.FLOAT, 4, "Age");
		s_employee.initField(3, AttrType.FLOAT, 10, "Salary");
		s_employee.initField(4, AttrType.INTEGER, 4, "DeptID");
		
		//initialize schema for department
		s_department = new Schema(4);
		s_department.initField(0, AttrType.INTEGER, 4, "DeptId");
		s_department.initField(1, AttrType.STRING, 50, "Name");
		s_department.initField(2, AttrType.FLOAT, 10, "MinSalary");
		s_department.initField(3, AttrType.FLOAT, 10, "MaxSalary");
		
		//Populate Employee and Department Data
		qep.populateTables(folderLocation);
		
		// run all the test cases
		System.out.println("\n" + "Running " + TEST_NAME + " .....");
		
		boolean status = PASS;
		status &= qep.test1();
		status &= qep.test2();
		status &= qep.test3();
		status &= qep.test4();
		
		// display final results
		System.out.println();
		if (status != PASS){
			System.out.println("Error(s) encountered during " + TEST_NAME + ".");
		}
		else {
			System.out.println("All " + TEST_NAME + " completed; verify output for correctness. ");
		}
	}
	
	/**
	 * Question 1:
	 * Display for each employee his ID, Name and Age
	 * 
	 */
	protected boolean test1() {
		
		System.out.println("\n Test 1 : Display for each employee his ID, Name and Age");
		
		saveCounts(null);
		FileScan scan = new FileScan(s_employee, employee);
		Projection pro = new Projection(scan, 0,1,2);
		pro.execute();
		saveCounts("qep-test1");
		
		System.out.println("\n\nTest 1 completed without exception");
		return PASS;
	}
	
	/**
	 * Display the Name for the departments with MinSalary = MaxSalary
	 *
	 */
	protected boolean test2(){
		
		System.out.println("\n Test 2 : Display the Name for the departments with MinSalary = MaxSalary");
		saveCounts(null);
		Predicate preds = new Predicate(AttrOperator.EQ, AttrType.FIELDNO, 2, AttrType.FIELDNO, 3);
		FileScan scan = new FileScan(s_department, department);
		Selection sel = new Selection(scan, preds);
		Projection pro = new Projection(sel, 1);
		pro.execute();
		saveCounts("qep-test2");
		
		System.out.println("\n\nTest 2 completed without exception");
		return PASS;
	}
	
	/**
	 * For each employee, display his Name and the Name of his department as well as the
	 * maximum salary of his department
	 * 
	 */
	protected boolean test3(){
		
		System.out.println("\n Test 3 : For each employee, display his Name and the Name of his department as well as the maximum salary of his department");
		saveCounts(null);
		
		HashJoin join = new HashJoin(new FileScan(s_employee, employee), new IndexScan(s_department, ixdeptId, department), 4,5);
		Projection pro = new Projection(join, 1,6,8);
		pro.execute();
		saveCounts("qep-test3");
		
		System.out.println("\n\nTest 3 completed without exception");
		return PASS;
	}
	
	/**
	 * Display the Name for each employee whose Salary is greater than the maximum salary
	 * of his department.
	 * 
	 */
	protected boolean test4() {
		
		System.out.println("\n Test 4 : Display the Name for each employee whose Salary is greater than the maximum salary of his department.");
		saveCounts(null);
		
		HashJoin join = new HashJoin(new FileScan(s_employee, employee), new IndexScan(s_department, ixdeptId, department), 4,5);
		Selection sel = new Selection(join, new Predicate(AttrOperator.GT, AttrType.FIELDNO, 3, AttrType.FIELDNO, 8));
		Projection pro = new Projection(sel, 1);
		pro.execute();
		saveCounts("qep-test4");

		return PASS;
	}
	
	public void populateTables(String folderLocation){
		
		System.out.println("Folder Location : " + folderLocation);
		initCounts();
		
		//create and populate the Employee table
		saveCounts(null);
		employee = new HeapFile(null);
		Tuple tuple = new Tuple(s_employee);
		
		BufferedReader br = null;
		int count = 0;
		try {
			String line;
			br = new BufferedReader(new FileReader(folderLocation + EMPLOYEE_FILE_NAME));
			
			while((line=br.readLine())!=null){
				if(count!=0){
			//		System.out.println(line);
					String[] parts = line.trim().split(",");
					if (parts.length == 5){
						int empId = Integer.parseInt(parts[0].trim());
						float age = Float.parseFloat(parts[2].trim());
						float salary = Float.parseFloat(parts[3].trim());
						int deptId = Integer.parseInt(parts[4].trim());
					
						tuple.setAllFields(empId, parts[1], age, salary, deptId);
						tuple.insertIntoFile(employee);
					}
				}
				count++;
			}
			System.out.println("Employee Records Inserted : " + count);
			saveCounts("employee");
		}
		catch(IOException e){
			e.printStackTrace();
		}
		finally {
			try{
				if(br!=null){
					br.close();
				}
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		
		//create and populate the Department table
		// create index on deptId
		saveCounts(null);
		department = new HeapFile(null);
		tuple = new Tuple(s_department);
		ixdeptId = new HashIndex(null);
		
		br = null;
		count = 0;
		try {
			String line;
			br = new BufferedReader(new FileReader(folderLocation + DEPARTMENT_FILE_NAME));
			
			while((line=br.readLine())!=null){
				if(count!=0){
					String[] parts = line.trim().split(",");
				//	System.out.println(line);
					if (parts.length==4){
						int deptId = Integer.parseInt(parts[0].trim());
						float minSal = Float.parseFloat(parts[2].trim());
						float maxSal = Float.parseFloat(parts[3].trim());
						
						tuple.setAllFields(deptId, parts[1], minSal, maxSal);
						RID rid = tuple.insertIntoFile(department);
						
						// create index on deptId
						ixdeptId.insertEntry(new SearchKey(deptId),rid);
					}
				}
				count++;
			}
			System.out.println("Department Records Inserted : " + count);
			saveCounts("department");
		}
		catch(IOException e){
			e.printStackTrace();
		}
		finally {
			try{
				if(br!=null){
					br.close();
				}
			}
			catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}