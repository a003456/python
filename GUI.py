
import pandas as pd
import chess.pgn
import warnings
import pygame
import chess
import chess.engine
import os
import sys


warnings.filterwarnings("ignore")




pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.float_format', '{:.2f}'.format)




directory_path = "D:\\algo_trading\\lichess"
os.chdir(directory_path)




STOCKFISH_PATH = "stk_fish/stockfish/stockfish-windows-x86-64-avx2.exe"






WIDTH, HEIGHT = 640, 640
SQUARE_SIZE = WIDTH // 8
WHITE, GRAY, GREEN, BLUE = (255, 255, 255), (125, 135, 150), (0, 255, 0), (70, 130, 180)


def load_pieces():
    pieces = {}
    for color in ['w', 'b']:
        for piece in ['p', 'r', 'n', 'b', 'q', 'k']:
            key = f"{color}{piece}"
            path = os.path.join("images", f"{key}.png")
            img = pygame.image.load(path)
            pieces[key] = pygame.transform.scale(img, (SQUARE_SIZE, SQUARE_SIZE))
    return pieces

def draw_board(screen, selected_square=None, legal_moves=[]):
    for row in range(8):
        for col in range(8):
            color = WHITE if (row + col) % 2 == 0 else GRAY
            rect = pygame.Rect(col*SQUARE_SIZE, row*SQUARE_SIZE, SQUARE_SIZE, SQUARE_SIZE)
            pygame.draw.rect(screen, color, rect)

    if selected_square is not None:
        col = chess.square_file(selected_square)
        row = 7 - chess.square_rank(selected_square)
        pygame.draw.rect(screen, BLUE, pygame.Rect(col*SQUARE_SIZE, row*SQUARE_SIZE, SQUARE_SIZE, SQUARE_SIZE), 4)

        for move in legal_moves:
            to_sq = move.to_square
            col = chess.square_file(to_sq)
            row = 7 - chess.square_rank(to_sq)
            pygame.draw.circle(screen, GREEN, (col * SQUARE_SIZE + SQUARE_SIZE//2, row * SQUARE_SIZE + SQUARE_SIZE//2), 10)

def draw_pieces(screen, board, piece_images, dragged_piece=None, dragged_pos=None):
    for square in chess.SQUARES:
        if dragged_piece and square == dragged_piece[0]:
            continue  # skip drawing dragged piece here

        piece = board.piece_at(square)
        if piece:
            row = 7 - chess.square_rank(square)
            col = chess.square_file(square)
            color = 'w' if piece.color == chess.WHITE else 'b'
            key = f"{color}{piece.symbol().lower()}"
            screen.blit(piece_images[key], (col * SQUARE_SIZE, row * SQUARE_SIZE))

    if dragged_piece:
        piece = dragged_piece[1]
        color = 'w' if piece.color == chess.WHITE else 'b'
        key = f"{color}{piece.symbol().lower()}"
        screen.blit(piece_images[key], (dragged_pos[0] - SQUARE_SIZE//2, dragged_pos[1] - SQUARE_SIZE//2))

def get_square_under_mouse(pos):
    x, y = pos
    col = x // SQUARE_SIZE
    row = 7 - (y // SQUARE_SIZE)
    if 0 <= col <= 7 and 0 <= row <= 7:
        return chess.square(col, row)
    return None

def display_status(screen, board, font):
    if board.is_checkmate():
        text = "Checkmate!"
    elif board.is_stalemate():
        text = "Stalemate!"
    elif board.is_check():
        text = "Check!"
    else:
        text = f"{'White' if board.turn else 'Black'} to move"

    surface = font.render(text, True, (0, 0, 0))
    screen.blit(surface, (10, 10))

def get_legal_moves(board, square):
    return [move for move in board.legal_moves if move.from_square == square]

def play_engine_move(board, engine):
    if board.is_game_over():
        return
    result = engine.play(board, chess.engine.Limit(time=0.1))
    board.push(result.move)





def main():
    pygame.init()
    screen = pygame.display.set_mode((WIDTH, HEIGHT))
    pygame.display.set_caption("Chess GUI")
    clock = pygame.time.Clock()
    font = pygame.font.SysFont(None, 30)

    board = chess.Board()
    piece_images = load_pieces()

    # Engine setup
    engine = chess.engine.SimpleEngine.popen_uci(STOCKFISH_PATH)

    selected_square = None
    legal_moves = []
    dragged_piece = None
    running = True

    while running:
        draw_board(screen, selected_square, legal_moves)
        draw_pieces(screen, board, piece_images, dragged_piece, pygame.mouse.get_pos() if dragged_piece else None)
        display_status(screen, board, font)
        pygame.display.flip()

        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                running = False

            elif event.type == pygame.MOUSEBUTTONDOWN:
                square = get_square_under_mouse(pygame.mouse.get_pos())
                if square is not None:
                    piece = board.piece_at(square)
                    if piece and piece.color == board.turn:
                        selected_square = square
                        legal_moves = get_legal_moves(board, square)
                        dragged_piece = (square, piece)

            elif event.type == pygame.MOUSEBUTTONUP:
                if dragged_piece:
                    from_square = dragged_piece[0]
                    to_square = get_square_under_mouse(pygame.mouse.get_pos())
                    move = chess.Move(from_square, to_square)
                    if move in board.legal_moves:
                        board.push(move)
                        selected_square = None
                        legal_moves = []
                        dragged_piece = None

                        # Engine responds
                        play_engine_move(board, engine)
                    else:
                        selected_square = None
                        legal_moves = []
                        dragged_piece = None

        clock.tick(60)

    engine.quit()
    pygame.quit()
    sys.exit()

if __name__ == "__main__":
    main()
