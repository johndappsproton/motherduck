import sys
from PyQt5.QtWidgets import QApplication, QWidget, QLabel, QLineEdit, QPushButton, QVBoxLayout
 
class UserInputApp(QWidget):
    def __init__(self):
        super().__init__()
        self.init_ui()
 
    def init_ui(self):
        self.setWindowTitle('User Input App')
        self.setGeometry(100, 100, 400, 200)
 
        self.label = QLabel('Enter text:')
        self.text_input = QLineEdit()
        self.save_button = QPushButton('Save to File')
        self.save_button.clicked.connect(self.save_to_file)
 
        layout = QVBoxLayout()
        layout.addWidget(self.label)
        layout.addWidget(self.text_input)
        layout.addWidget(self.save_button)
 
        self.setLayout(layout)
 
    def save_to_file(self):
        text = self.text_input.text()
        with open('user_input.txt', 'a+') as file:
            file.write(text + '\n')
        print('Text saved to file.')
 
if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = UserInputApp()
    window.show()
    sys.exit(app.exec_())