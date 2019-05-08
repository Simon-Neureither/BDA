import scala.collection.mutable.ListBuffer

class Possibilities() {
  var field = scala.collection.mutable.Map[Int, scala.collection.mutable.ListBuffer[Int]]();
  
  def set(x : Int, y : Int)
  {
    field(x) += y;
  }
  
  def del(x : Int, y : Int)
  {
    field(x) -= y;
  }
  
  def num(x : Int)
  {
    field(x).size;
  }
}

class Field() {
  
  var field = ListBuffer(
      ListBuffer(0,2,3,4,5,6,7,8,0),
      ListBuffer(0,0,0,0,0,0,0,0,0),
      ListBuffer(0,4,0,0,0,0,0,0,0),
      ListBuffer(0,5,0,0,0,0,0,0,0),
      ListBuffer(0,6,0,0,0,0,0,0,0),
      ListBuffer(0,7,0,0,0,0,0,0,0),
      ListBuffer(0,8,0,0,0,0,0,0,0),
      ListBuffer(0,9,0,0,0,0,0,0,0),
      ListBuffer(0,0,0,0,0,0,0,0,0)
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
  
  
  def solve : Field = {
    
    
    var f = copy
    
    var idx = 0;
    
    var solved = false;
   // while (solved)
    {
     // solved = false;
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
    }
     
    f;
    
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
      
      println(f.getAllowed(0, 0).toString());
      
      println(f.toString());
      println(f2.toString());
      
    }  
}  
