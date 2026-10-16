#!/usr/bin/env python3
"""
Captcha Solver for eprocure.gov.in
----------------------------------
Uses a custom trained PyTorch character-level CNN model.
Slices the captcha bounding box into 6 equal chunks and classifies them.
"""

import base64
import os
import cv2
import numpy as np
import torch
import torch.nn as nn

MODEL_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "char_cnn.pth")
CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
IDX_TO_CHAR = {idx: char for idx, char in enumerate(CHARS)}
NUM_CLASSES = len(CHARS)

class CharCNN(nn.Module):
    def __init__(self):
        super(CharCNN, self).__init__()
        self.conv = nn.Sequential(
            nn.Conv2d(1, 16, kernel_size=3, padding=1),
            nn.BatchNorm2d(16),
            nn.ReLU(),
            nn.MaxPool2d(2, 2), # 16 x 10 x 10
            
            nn.Conv2d(16, 32, kernel_size=3, padding=1),
            nn.BatchNorm2d(32),
            nn.ReLU(),
            nn.MaxPool2d(2, 2), # 32 x 5 x 5
        )
        self.fc = nn.Sequential(
            nn.Linear(32 * 5 * 5, 128),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(128, NUM_CLASSES)
        )
        
    def forward(self, x):
        x = self.conv(x)
        x = x.view(x.size(0), -1)
        return self.fc(x)

# Global model instance
_MODEL = None

def _initialize_model():
    global _MODEL
    if _MODEL is None:
        model = CharCNN()
        if torch.cuda.is_available():
            model.load_state_dict(torch.load(MODEL_PATH))
            model = model.cuda()
        else:
            model.load_state_dict(torch.load(MODEL_PATH, map_location=torch.device('cpu')))
        model.eval()
        _MODEL = model

def preprocess_and_slice(img, target_size=(20, 20)):
    if img is None or img.shape[2] < 4:
        raise ValueError("Image must be loaded with alpha channel (UNCHANGED/BGRA).")
        
    rgb = img[:, :, :3]
    alpha = img[:, :, 3]
    white_bg = np.ones_like(rgb) * 255
    alpha_mask = alpha[:, :, np.newaxis] / 255.0
    blended = (rgb * alpha_mask + white_bg * (1.0 - alpha_mask)).astype(np.uint8)
    
    gray = cv2.cvtColor(blended, cv2.COLOR_BGR2GRAY)
    _, thresh = cv2.threshold(gray, 200, 255, cv2.THRESH_BINARY_INV)
    
    coords = cv2.findNonZero(thresh)
    if coords is None:
        return None
    gx, gy, gw, gh = cv2.boundingRect(coords)
    
    chunk_w = gw / 6.0
    slices = []
    for i in range(6):
        x1 = int(gx + i * chunk_w)
        x2 = int(gx + (i + 1) * chunk_w)
        char_crop = thresh[gy:gy+gh, x1:x2]
        
        c_coords = cv2.findNonZero(char_crop)
        if c_coords is not None:
            cx, _, cw, _ = cv2.boundingRect(c_coords)
            char_crop = char_crop[:, cx:cx+cw]
            
        resized = cv2.resize(char_crop, target_size, interpolation=cv2.INTER_AREA)
        slices.append(resized.astype(np.float32) / 255.0)
        
    return slices

def solve_captcha(image_source):
    """
    Solves the captcha using the custom PyTorch CNN.
    """
    _initialize_model()
    
    img = None
    if isinstance(image_source, str):
        if image_source.startswith("data:image"):
            # Base64 string
            header, encoded = image_source.split(",", 1) if "," in image_source else ("", image_source)
            img_bytes = base64.b64decode(encoded)
            nparr = np.frombuffer(img_bytes, np.uint8)
            img = cv2.imdecode(nparr, cv2.IMREAD_UNCHANGED)
        else:
            # File path
            img = cv2.imread(image_source, cv2.IMREAD_UNCHANGED)
    elif isinstance(image_source, bytes):
        nparr = np.frombuffer(image_source, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_UNCHANGED)
    elif isinstance(image_source, np.ndarray):
        img = image_source
        
    if img is None:
        raise ValueError("Failed to load or parse image source.")
        
    slices = preprocess_and_slice(img)
    if slices is None or len(slices) != 6:
        return None
        
    # Stack slices and predict
    tensors = torch.stack([torch.tensor(s, dtype=torch.float32).unsqueeze(0) for s in slices])
    
    if torch.cuda.is_available():
        tensors = tensors.cuda()
        
    with torch.no_grad():
        outputs = _MODEL(tensors)
        # Apply Softmax to get probabilities
        probs = torch.softmax(outputs, dim=1)
        # Find maximum probability and corresponding class index for each character slice
        max_probs, preds = torch.max(probs, dim=1)
        
        preds = preds.cpu().numpy()
        max_probs = max_probs.cpu().numpy()
        
    predicted_str = "".join([IDX_TO_CHAR[p] for p in preds])
    # Total captcha confidence is the average probability of all 6 characters
    mean_confidence = float(np.mean(max_probs))
    return predicted_str, mean_confidence

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        img_path = sys.argv[1]
        try:
            print(f"Solved Captcha: {solve_captcha(img_path)}")
        except Exception as e:
            print(f"Error: {e}")
    else:
        print("Usage: python3 captcha_solver.py <path_to_captcha_png>")
