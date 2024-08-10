package store

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
)

type Encryption struct {
	secret string
}

func NewEncryption(secret string) *Encryption {
	return &Encryption{
		secret: secret,
	}
}

func (e *Encryption) Encode(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

func (e *Encryption) Decode(s string) ([]byte, error) {
	data, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// Generate a new random IV
func (e *Encryption) generateIV() ([]byte, error) {
	iv := make([]byte, aes.BlockSize)
	_, err := io.ReadFull(rand.Reader, iv)
	if err != nil {
		return nil, err
	}
	return iv, nil
}

// Encrypt method is to encrypt or hide any classified text
func (e *Encryption) Encrypt(text string) (string, error) {
	block, err := aes.NewCipher([]byte(e.secret))
	if err != nil {
		return "", err
	}
	plainText := []byte(text)
	iv, err := e.generateIV()
	if err != nil {
		return "", err
	}
	cfb := cipher.NewCFBEncrypter(block, iv)
	cipherText := make([]byte, len(plainText))
	cfb.XORKeyStream(cipherText, plainText)
	return e.Encode(append(iv, cipherText...)), nil
}

// Decrypt method is to extract back the encrypted text
func (e *Encryption) Decrypt(text string) (string, error) {
	block, err := aes.NewCipher([]byte(e.secret))
	if err != nil {
		return "", err
	}
	cipherText, err := e.Decode(text)
	if err != nil {
		return "", err
	}
	if len(cipherText) < aes.BlockSize {
		return "", errors.New("ciphertext too short")
	}
	iv, cipherText := cipherText[:aes.BlockSize], cipherText[aes.BlockSize:]
	cfb := cipher.NewCFBDecrypter(block, iv)
	plainText := make([]byte, len(cipherText))
	cfb.XORKeyStream(plainText, cipherText)
	return string(plainText), nil
}
