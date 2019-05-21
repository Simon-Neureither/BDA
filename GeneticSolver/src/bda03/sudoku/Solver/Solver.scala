package bda03.sudoku.Solver
import bda03.sudoku.gui.Board

class Solver(val settings: Settings) {

  def solve(board: Board): Board = {
    
    var p = Population(board, settings.populationSize)
    var best = board
    
    var count = 0
    while (count < settings.maxGenerations && best.score.getOrElse(Board.WorstScore) != 0) {
      p = p.nextGen(settings.minMutationProbability)
      if (p.best < best.score.getOrElse(Board.WorstScore))
        best = p.samples(0)
      count += 1
    }
    
    best
  }

}
