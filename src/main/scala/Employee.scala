
case class Employee(
                     educationLevel: String,
                     joiningYear: Int,
                     city: String,
                     paymentTier: Int,
                     age: Int,
                     gender: String,
                     benchStatus: Int,
                     experienceInCurrentDomain: Int,
                     hasLeftCompany: Int
                   )

object Employee {
  // Companion object for Employee



  def createFromCsv(line: String): Employee = {
    val attributes = line.split(",")
    Employee(
      attributes(0),
      attributes(1).toInt,
      attributes(2),
      attributes(3).toInt,
      attributes(4).toInt,
      attributes(5),
      convertBenchStatus(attributes(6)),
      attributes(7).toInt,
      attributes(8).toInt
    )
  }

  def convertBenchStatus(benchStatus: String): Int =
    if ("Yes".equalsIgnoreCase(benchStatus)) 1 else 0
}

