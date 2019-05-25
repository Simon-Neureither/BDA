
import scala.io.Source
import java.io._
import scala.collection.mutable.ListBuffer

class Board {
    var board = List(
    ListBuffer(0,0,0,0,0,0,0,0,0),
    ListBuffer(0,0,0,0,0,0,0,0,0),
    ListBuffer(0,0,0,0,0,0,0,0,0),
    ListBuffer(0,0,0,0,0,0,0,0,0),
    ListBuffer(0,0,0,0,0,0,0,0,0),
    ListBuffer(0,0,0,0,0,0,0,0,0),
    ListBuffer(0,0,0,0,0,0,0,0,0),
    ListBuffer(0,0,0,0,0,0,0,0,0),
    ListBuffer(0,0,0,0,0,0,0,0,0),
  )
  
  def this(b : ListBuffer[ListBuffer[Int]])
  {      
      this
      
      board = List[ListBuffer[Int]](
          b(0),
          b(1),
          b(2),
          b(3),
          b(4),
          b(5),
          b(6),
          b(7),
          b(8),
      );
  }
    
  def this(b : List[ListBuffer[Int]])
  {      
      this
      board = b;
  }
    
  def isSolved : Boolean = {
    
    for (x <- 0 to 8)
    {
      for (y <- 0 to 8)
      {
        if (board(x)(y) == 0)
          return false;
      }
    }
    return true;
  }
  
  def getNumberOfFreeFields : Int = {
    
    var n = 0;
    
    for (x <- 0 to 8)
    {
      for (y <- 0 to 8)
      {
        if (board(x)(y) == 0)
          n += 1;
      }
    }
    
    n;
  }
    
  override def toString = {
    
    var str = "";
    
    for (list <- board) {
      
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
  
  override def equals(that: Any): Boolean =
    that match {
        case that: Board => 
          {
            board.equals(that.board)
          }
        case _ => false
 }

}

object Board {
  def EmptyBoard() : ListBuffer[ListBuffer[Int]] =
  {
    ListBuffer(
      ListBuffer(0,0,0,0,0,0,0,0,0),
      ListBuffer(0,0,0,0,0,0,0,0,0),
      ListBuffer(0,0,0,0,0,0,0,0,0),
      ListBuffer(0,0,0,0,0,0,0,0,0),
      ListBuffer(0,0,0,0,0,0,0,0,0),
      ListBuffer(0,0,0,0,0,0,0,0,0),
      ListBuffer(0,0,0,0,0,0,0,0,0),
      ListBuffer(0,0,0,0,0,0,0,0,0),
      ListBuffer(0,0,0,0,0,0,0,0,0),
    )
  }
  
  def Copy(b : Board) : Board =
  {
     val board = List[ListBuffer[Int]](
          b.board(0).to[ListBuffer],
          b.board(1).to[ListBuffer],
          b.board(2).to[ListBuffer],
          b.board(3).to[ListBuffer],
          b.board(4).to[ListBuffer],
          b.board(5).to[ListBuffer],
          b.board(6).to[ListBuffer],
          b.board(7).to[ListBuffer],
          b.board(8).to[ListBuffer],
      );
    
    new Board(board);
    
  }
  
  def FromFile (filename : String) =
  {
    
    val board = EmptyBoard
    var row = 0;
    var column = 0;
    Source.fromFile(filename).foreach (
        c => {
          
          if (c == '-' || (c >= '1' && c <= '9'))
          {
            if (c == '-')
              board(row)(column) = 0;
            else
              board(row)(column) = c - '0';
            
            column += 1
            
            row = if (column == 9) row + 1 else row
            column = column % 9
          }
        }
    )
    
    
    new Board(board)
  }
  
  def FromString(string : String) =
  {
    val board = EmptyBoard
    var row = 0;
    var column = 0;
    string.foreach (
        c => {
          
          if (c == '-' || (c >= '1' && c <= '9'))
          {
            if (c == '-')
              board(row)(column) = 0;
            else
              board(row)(column) = c - '0';
            
            column += 1
            
            row = if (column == 9) row + 1 else row
            column = column % 9
          }
        }
    )
    
    
    new Board(board)
  }
}

object main
{
    def main(args:Array[String]){  
      
      var board : Board = null;
      
      if (args.size >= 2)
      {
        if (args(0).equals("-s"))
          board = Board.FromString(args(1));
        else if (args(0).equals("-f"))
          board = Board.FromFile(args(1));
        else
          System.err.println("Unknown command '" + args(0) + "'!");
      }
      else
      {
        System.err.println("Not enough parameters!");
      }
      
      var to_solve = 1;
      if (args.size > 2)
      {
        if (args(3).equals("-n"))
          to_solve = Integer.parseInt(args(4));
      }
      
      if (board != null)
      {
          val solver = new Solver(board);
          val solved = solver.solve(to_solve);
          
          if (solved.size > 0)
            println(solved(0))
          else
            println("No solution found!");
      }
    }  
}