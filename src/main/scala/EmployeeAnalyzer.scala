import scala.io.Source
import Employee.createFromCsv
import scala.util.{Try, Success, Failure}


object EmployeeAnalyzer extends App {

  val filePath = "C:\\Users\\c22832b\\Downloads\\Employee.csv"
  val employeeList = loadData(filePath)
  loadData(filePath) match {
    case Success(employeeList) =>
      val preprocessedEmployeeList = preprocessData(employeeList)


      println("----------------------------")
      println("|          REPORT          |")
      println("----------------------------")
      calculateStatistics(preprocessedEmployeeList)
      analyzeRetentionFactors(preprocessedEmployeeList)
      segmentEmployees(preprocessedEmployeeList)


    case Failure(exception) =>
      println(s"Error loading data: ${exception.getMessage}")
  }

  // Function to load data from a CSV file
  def loadData(filePath: String): Try[List[Employee]] = Try{
    val bufferedSource = Source.fromFile(filePath) // Open the file for reading
    val lines = bufferedSource.getLines().drop(1).toList // Read lines, skip the header, and convert to a list
    bufferedSource.close()
    lines.map(Employee.createFromCsv) //Map each line to an employee object. it will apply the function "createEmployeeFromCsv"
    // to each element of the list, and will produce a new list of Employee objects.
  }


  //Function to handle missing data
      def handleMissingData(employee: Employee): Employee = {
        val updatedPaymentTier = if (employee.paymentTier < 0) 0 else employee.paymentTier
        employee.copy(paymentTier = updatedPaymentTier)
      }

      // Function to preprocess data
      def preprocessData(employeeList: List[Employee]): List[Employee] = {
        employeeList.map(handleMissingData)
          .filter(_.age >= 0)
      }

      //Functions to calculate statistics:
      def calculateStatistics(employeeList: List[Employee]): Unit = {

        println("----------------------------")
        println("Statistical Analysis :")
        println("----------------------------")

        //Average age:
        val averageAge = employeeList.map(_.age).sum.toDouble / employeeList.length
        println(s"Average Age:$averageAge")

        //Gender Count:
        val maleCount = employeeList.count(_.gender.equalsIgnoreCase("Male"))
        val femaleCount = employeeList.length - maleCount
        println(s"Gender Ratio (Male/Female): $maleCount/$femaleCount")

        //Education Level:
        val educationLevelDistribution = employeeList.groupBy(_.educationLevel).view.mapValues(_.size)
        println("Education Level Distribution:")
        educationLevelDistribution.foreach {
          case (level, count) => println(s"$level : $count")
        }

      }

      //Function to analyze factors affecting employee retention
      def analyzeRetentionFactors(employeeList: List[Employee]): Unit = {

        println("----------------------------")
        println("Retention Factor Analysis :")
        println("----------------------------")

        //Payment Tier analysis:
        val paymentTierDistribution = employeeList.groupBy(_.paymentTier).view.mapValues(_.size)
        println("Payenment Tier Analysis:")
        paymentTierDistribution.foreach {
          case (tier, count) => println(s"Payment Tier $tier: $count")
        }

        //Bench Status Analysis:
        val benchStatusDistribution = employeeList.groupBy(_.benchStatus).view.mapValues(_.size)
        println("Bench Status Analysis :")
        benchStatusDistribution.foreach {
          case (status, count) => println(s"Bench Status $status : $count")
        }

        //Experience in current domain analysis:
        val experienceDistribution = employeeList.groupBy(_.experienceInCurrentDomain).view.mapValues(_.size)
        println("Experience In Current Domain Analysis:")
        experienceDistribution.foreach {
          case (exp, count) => println(s"Experience in Current Domain - $exp : $count")
        }
      }

      //Functions to segment employees based on attributes:
      def segmentEmployees(employeeList: List[Employee]): Unit = {

        println("----------------------------")
        println("Segmentation Analysis :")
        println("----------------------------")

        //Segment Employees based on city
        val citySegmentation = employeeList.groupBy(_.city).view.mapValues(_.size)
        println("Segmentation Analysis: (City Wise)")
        citySegmentation.foreach {
          case (city, count) => println(s"$city : $count")
        }

        //Segment Employees based on Education Level:
        val educationSegmentation = employeeList.groupBy(_.educationLevel)
        println("Segmentation Analysis: (Education Level wise:)")
        educationSegmentation.foreach {
          case (level, employees) =>
            val educationlevelCount = employees.size
            println(s"$level : $educationlevelCount")
        }
      }
}
