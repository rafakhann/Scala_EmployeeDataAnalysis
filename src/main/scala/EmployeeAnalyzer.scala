import scala.io.Source
import Employee.createFromCsv
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
//import scala.concurrent.duration._


object EmployeeAnalyzer extends App {

  implicit val ec: ExecutionContext = ExecutionContext.global

  private val filePath = "C:\\Users\\c22832b\\Downloads\\Employee.csv"
  val employeeList = loadData(filePath)
  loadData(filePath) match {
    case Success(employeeList) =>
      val preprocessedEmployeeList = preprocessData(employeeList)


      println("----------------------------")
      println("|          REPORT          |")
      println("----------------------------")

      val statisticsFuture: Future[List[Any]] = calculateStatistics(preprocessedEmployeeList)
      val retentionFactorsFuture: Future[List[Any]] = analyzeRetentionFactors(preprocessedEmployeeList)
      val segmentationFuture: Future[List[Any]] = segmentEmployees(preprocessedEmployeeList)

      // Await.result(awaitable = statisticsFuture, atMost = 10.seconds)
      // println("---------------11111111111111111" )


      //Combining the futures into a single future for easier handling, waiting for all the futures to complete execution
      Future.sequence(Seq(statisticsFuture, retentionFactorsFuture, segmentationFuture)).onComplete {
        case Success(_) => generateReport(statisticsFuture,retentionFactorsFuture,segmentationFuture)

        case Failure(exception) =>
          println(s"Error during concurrent processing: ${exception.getMessage}")
      }
      Thread.sleep(5000)


    case Failure(exception) =>
      println(s"Error loading data: ${exception.getMessage}")
  }

  // Function to load data from a CSV file
  private def loadData(filePath: String): Try[List[Employee]] = Try{
    val bufferedSource = Source.fromFile(filePath) // Open the file for reading
    val lines = bufferedSource.getLines().drop(1).toList // Read lines, skip the header, and convert to a list
    bufferedSource.close()
    lines.map(Employee.createFromCsv) //Map each line to an employee object. it will apply the function "createEmployeeFromCsv"
    // to each element of the list, and will produce a new list of Employee objects.
  }


  //Function to handle missing data
  private def handleMissingData(employee: Employee): Employee = {
    val updatedPaymentTier = if (employee.paymentTier < 0) 0 else employee.paymentTier
    employee.copy(paymentTier = updatedPaymentTier)
  }

  // Function to preprocess data
  private def preprocessData(employeeList: List[Employee]): List[Employee] = {
    employeeList.map(handleMissingData)
      .filter(_.age >= 0)
  }

  //Functions to calculate statistics:
  private def calculateStatistics(employeeList: List[Employee]): Future[List[Any]] = Future{
    //Average age:
    val averageAge = employeeList.map(_.age).sum.toDouble / employeeList.length
    //Gender Count:
    val maleCount = employeeList.count(_.gender.equalsIgnoreCase("Male"))
    val femaleCount = employeeList.length - maleCount
    //Education Level:
    val educationLevelDistribution = employeeList.groupBy(_.educationLevel).view.mapValues(_.size).toList

    List(averageAge,maleCount,femaleCount,educationLevelDistribution)
  }


  //Function to analyze factors affecting employee retention
  private def analyzeRetentionFactors(employeeList: List[Employee]): Future[List[Any]] = Future{
    //Payment Tier analysis:
    val paymentTierDistribution = employeeList.groupBy(_.paymentTier).view.mapValues(_.size).toList
    //Bench Status Analysis:
    val benchStatusDistribution = employeeList.groupBy(_.benchStatus).view.mapValues(_.size).toList
    //Experience in current domain analysis:
    val experienceDistribution = employeeList.groupBy(_.experienceInCurrentDomain).view.mapValues(_.size).toList

    List(paymentTierDistribution, benchStatusDistribution, experienceDistribution)
  }

  //Functions to segment employees based on attributes:
  private def segmentEmployees(employeeList: List[Employee]): Future[List[Any]] = Future{
    //Segment Employees based on city
    val citySegmentation = employeeList.groupBy(_.city).view.mapValues(_.size).toList
    //Segment Employees based on Education Level:
    val educationSegmentation = employeeList.groupBy(_.educationLevel).view.mapValues(_.size).toList

    List(citySegmentation, educationSegmentation)
  }

  def generateReport(statistical : Future[List[Any]], retention : Future[List[Any]], segmentation: Future[List[Any]]): Unit = {
    println("----------------------------")
    println("Segmentation Analysis :")
    println("----------------------------")
    println(statistical.toString)
    println("----------------------------")
    println("Retention Factor Analysis :")
    println("----------------------------")
    println(retention.toString)
    println("----------------------------")
    println("Segmentation Analysis :")
    println("----------------------------")
    println(segmentation.toString)
  }
}
