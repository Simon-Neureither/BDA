import scala.collection.mutable.ListBuffer

class Possibility() {
  //var field = scala.collection.mutable.Map[Int, scala.collection.mutable.ListBuffer[Int]]();
  
  var field = scala.collection.mutable.ListBuffer[Int]();
  
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

class BestPossibility() extends Possibility
{
  var x : Int = 0;
  var y : Int = 0;
  
  def setXY(x_ : Int, y_ : Int) =
  {
    x = x_;
    y = y_;
  }
}

class Possibilities()
{  
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
  
  def getLeast() : BestPossibility =
  {
    var best : BestPossibility = null;
    for ((idx, possibility) <- field)
    {
      if (best == null || best.number() > possibility.number())
      {
        best = new BestPossibility;
        best.field = possibility.field;
        best.y = idx / 9;
        best.x = idx - best.y * 9;
      }
    }
    
    best;
  }
  
}

class Field() {
  
  var field = ListBuffer(
      ListBuffer(8,7,6,9,0,0,0,0,0),
      ListBuffer(0,1,0,0,0,6,0,0,0),
      ListBuffer(0,4,0,3,0,5,8,0,0),
      ListBuffer(4,0,0,0,0,0,2,1,0),
      ListBuffer(0,9,0,5,0,0,0,0,0),
      ListBuffer(0,5,0,0,4,0,3,0,6),
      ListBuffer(0,2,9,0,0,0,0,0,8),
      ListBuffer(0,0,4,6,9,0,1,7,3),
      ListBuffer(0,0,0,0,0,1,0,0,4)
  )
  
  override def toString = {
    
    var str = "";
    
    for (list <- field) {
      
      var i = 0;
      for (x <- list)
      {
        i = i + 1;
        str = str.concat(if (x == 0) "-" else x.toString());
        if (i % 3 == 0)
          str = str.concat(if (i == 9) "\r\n" else "|");
      }
    }
    
    str;
  }
  
  def isFixed(x : Int, y : Int) =
  {
    get(x, y) != 0;
  }
  
  def get(x : Int, y : Int) =
  {
    field(x)(y);
  }
  
  def sector(x : Int, y : Int) =
  {
    var s = 0;
    
    x match {
      case 0 | 1 | 2 => s+=0;
      case 3 | 4 | 5 => s+=1;
      case 6 | 7 | 8 => s+=2;
    }
    
    y match {
      case 0 | 1 | 2 => s+=0;
      case 3 | 4 | 5 => s+=3;
      case 6 | 7 | 8 => s+=6;
    }
    
    s;
  }
  
  def getAllowed(x : Int, y : Int) =
  {
    var allowed = ListBuffer(1,2,3,4,5,6,7,8,9);
    for (x_ <- 0 to 8)
    {
      if (get(x_, y) != 0)
      {
        allowed -= get(x_,y);
      }
    }
    
    for (y_ <- 0 to 8)
    {
      if (get(x, y_) != 0)
      {
        allowed -= get(x, y_);
      }
    }
        
    var x_min = (x / 3) * 3;
    var x_max = x_min + 2;
    
    var y_min = (y / 3) * 3;
    var y_max = y_min + 2;
    
    for (x_ <- x_min to x_max)
      for (y_ <- y_min to y_max)
      {
        if (get(x_, y_) != 0)
        {
          allowed -= get(x_,y_);
        }
      }
    
    allowed;
  }
  
  def set(x : Int, y : Int, n : Int) =
  {
    field(x)(y) = n;
  }
  
  def solve_one_step_ : Field = {
    var f = copy;
    
    var solved = false;
    for (x <- 0 to 8)
      for (y <- 0 to 8)
      {
        var allowed = if (get(x,y) != 0) ListBuffer() else getAllowed(x, y);
        if (allowed.size == 1 && !solved)
        {
          solved = true;
          f.set(x,y, allowed(0));
          f = f.solve;
        }
      }
    
    if (!solved)
      f = null;
    
    f;
  }
  
  def is_finished : Boolean =
  {
    for (x <- 0 to 8)
      for (y <- 0 to 8)
      {
        if (get(x,y) == 0)
          return false;
      }
    
    true;
  }
  
  
  def solve_one_step_in = {
    var found = true;
    while (found)
    {
      found = false;
      for (x <- 0 to 8)
        for (y <- 0 to 8)
        {
          var allowed = if (get(x,y) != 0) ListBuffer() else getAllowed(x, y);
          if (allowed.size == 1)
          {
            this.set(x,y, allowed(0));
            found = true;
          }
        }
    }
    
  }
  
  def solve_one_step : Field = {
    var f = copy;
    
    
    var solved = false;
    var fields = new ListBuffer[Field];
   // fields += f;
    
    f.solve_one_step_in;
    
    var possibilities = f.make_possibilities;
    
    if (f.is_finished)
      return f;
    
    if (possibilities == null || possibilities.getLeast() == null)
      return null;
    
    
    var possibility = possibilities.getLeast();
    if (possibility == null)
    {
      var x = 3;
      x += 1;
    }
    for (i <- 0 to possibility.number() - 1)
    {
      var field = copy;
      field.set(possibility.x, possibility.y, possibility.get()(i));
      fields += field;
    }
    
    for (i <- 0 to fields.size - 1)
    {
      var field = fields(i);
      if (field != null)
      {
        System.out.println("Trying to solve field:\r\n" + field);
        field = field.solve_one_step;
        
        if (field != null && field.is_finished)
          return field;
        else if (field != null)
        {
          System.out.println("Failed solving field:\r\n" + field);
        }
      }
    }
    
    null;
  }
  
  
  def is_solved : Boolean = {
    var solved = true;
     for (x <- 0 to 8)
      for (y <- 0 to 8)
      {
        if (get(x, y) == 0)
        {
          solved = false;
        }
      }
     
     solved;
  }
  
  
  def make_possibilities : Possibilities =
  {
    var possibilities = new Possibilities;
    
    System.out.println(is_finished);
    
    for (x <- 0 to 8)
      for (y <- 0 to 8)
      {
        var c = get(x,y);
        if (c == 0) 
        {
          val buffer = getAllowed(x, y);
          if (buffer.size == 0)
          {
            return null;
          }
          else
          {
            for (n <- 0 to buffer.size - 1)
              possibilities.set(x, y, buffer(n));
          }
        }
      }
    possibilities;
  }
  
  
  def solve : Field = {
    
    
    var f = copy
    
    var idx = 0;
    
    var last_stable = copy;
    var current = copy;
    
    /*
    while (current != null && !current.is_finished)
    {
      last_stable = current;
      current = last_stable.solve_one_step;
    }
    System.out.println("LAST STABLE");
    System.out.println(current);
    last_stable;
    */
    
    
    current.solve_one_step;
    
    
     
    
  }
  
  def copy() = {
        
    var n = new Field;
    
    for (x <- 0 to 8)
      for (y <- 0 to 8)
      {
        n.set(x, y, get(x,y));
      }
    
    n;
  }
  
}



object ScalaExample{  
    def main(args:Array[String]){  
      
      var f = new Field();
      
      var f2 = f.solve
            
      println(f.toString());
      println(f2.toString());
      
    }  
}  
