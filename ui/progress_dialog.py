from PyQt6.QtWidgets import QDialog, QProgressBar, QVBoxLayout, QLabel, QPushButton, QHBoxLayout
from PyQt6.QtCore import Qt, pyqtSignal

class ProgressDialog(QDialog):
    cancelled = pyqtSignal()  # 添加取消信号
    
    def __init__(self, parent=None, title="处理中"):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setFixedSize(400, 120)
        self.setWindowModality(Qt.WindowModality.ApplicationModal)
        
        layout = QVBoxLayout(self)
        
        self.label = QLabel("准备中...")
        layout.addWidget(self.label)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        layout.addWidget(self.progress_bar)
        
        # 添加取消按钮
        button_layout = QHBoxLayout()
        self.cancel_button = QPushButton("取消")
        self.cancel_button.clicked.connect(self.on_cancel)
        button_layout.addStretch()
        button_layout.addWidget(self.cancel_button)
        layout.addLayout(button_layout)
        
        self.is_cancelled = False
    
    def update_progress(self, value, message):
        self.progress_bar.setValue(value)
        self.label.setText(message)
    
    def on_cancel(self):
        self.is_cancelled = True
        self.cancel_button.setEnabled(False)
        self.label.setText("正在取消...")
        self.cancelled.emit() 