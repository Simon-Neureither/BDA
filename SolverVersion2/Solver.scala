
import scala.collection.mutable.ListBuffer


class Solver(var board : Board) {
  
  
  
  def solve(to_solve : Int = 1) : ListBuffer[Board] =
  {
    if (board.isSolved)
    {
      return ListBuffer[Board](board);
    }
    
    val possibilities = make_possibilities
    
    if (possibilities == null)
    {
      return ListBuffer[Board]();
    }
    
    val possibility = possibilities.getLeast();
    
    if (possibility == null)
      return ListBuffer[Board]();
    
    
    val boards = new ListBuffer[Board];
    
    for (i <- 0 to possibility.number() - 1)
    {
      val b = Board.Copy(board);
      b.board(possibility.x)(possibility.y) = possibility.get()(i);
            
      boards += b;
    }
    
    val solvedBoards = new ListBuffer[Board];
    
    var solved_n = 0;

    boards.foreach(
      board => {
        
        if (solved_n < to_solve)
        {
          val solved = new Solver(board).solve(to_solve - solved_n);
          
          solved.foreach(
              b => {
                if (b.isSolved)
                  solved_n += 1;
              }
          )
          
          solvedBoards.insertAll(0, solved);
        }
      }
    )
    solvedBoards;
  }
  
  def make_possibilities : BoardPossibilities =
  {
    var possibilities = new BoardPossibilities;
        
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
    
  def get(x : Int, y : Int) =
  {
    board.board(x)(y);
  }
  
  
}