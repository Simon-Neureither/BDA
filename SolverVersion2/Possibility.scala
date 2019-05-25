import scala.collection.mutable.ListBuffer


class Possibility
{
  val field = scala.collection.mutable.ListBuffer[Int]();
  
  def set(n : Int) =
  {
    field += n;
  }
  
  def reset(n : Int) = 
  {
    field -= n;
  }
  
  def number() =
  {
    field.size;
  }
  
  def get() : ListBuffer[Int] = {
    field;
  }
}

case class FieldPossibility(x : Int, y : Int) extends Possibility
{
}

class BoardPossibilities {
  
  val field = scala.collection.mutable.Map[Int, Possibility]();
  
  def init(x : Int, y : Int)
  {
    val idx = x + y * 9;
    
    if (!field.contains(idx))
      field(idx) = new Possibility;
  }
  
  def set(x : Int, y : Int, n : Int)
  {
    val idx = x + y * 9;
    init(x,y);
    
    field(idx).set(n);
  }
  
  def remove(x : Int, y : Int, n : Int)
  {
    val idx = x + y * 9;
    init(x,y);
    
    field(idx).reset(n);
  }
  
  def getLeast() : FieldPossibility =
  {
    var best : FieldPossibility = null;
    for ((idx, possibility) <- field)
    {
      if (best == null || best.number() > possibility.number())
      {
        val y = idx / 9;
        val x = idx - y * 9;
        best = FieldPossibility(x, y);
        
        val number = possibility.number();
        best.field.insertAll(0, possibility.field)
      }
    }
    
    best;
  }  
  
}