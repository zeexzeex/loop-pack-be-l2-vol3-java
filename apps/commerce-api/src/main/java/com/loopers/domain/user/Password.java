package com.loopers.domain.user;

import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.util.Objects;

public class Password {
    private static final int MIN_LENGTH = 8;
    private static final int MAX_LENGTH = 16;
    private static final PasswordEncoder passwordEncoder = new BCryptPasswordEncoder();

    private final String value;

    private Password(String value) {
        this.value = value;
    }

    public static Password of(String rawPassword, BirthDate birthDate) {
        validateLength(rawPassword);
        if (birthDate == null) {
            throw new IllegalArgumentException("생년월일은 null일 수 없습니다.");
        }
        validateNotContainsBirthDate(rawPassword, birthDate);
        return new Password(rawPassword);
    }

    private static void validateLength(String password) {
        if (password == null || password.isBlank()) {
            throw new IllegalArgumentException("비밀번호는 비어있을 수 없습니다.");
        }
        if (password.length() < MIN_LENGTH) {
            throw new IllegalArgumentException("비밀번호는 최소 8자 이상이어야 합니다.");
        }
        if (password.length() > MAX_LENGTH) {
            throw new IllegalArgumentException("비밀번호는 최대 16자 이하여야 합니다.");
        }
    }

    private static void validateNotContainsBirthDate(String password, BirthDate birthDate) {
        String birthDateValue = birthDate.value();
        
        // yyyy-MM-dd 형식
        if (password.contains(birthDateValue)) {
            throw new IllegalArgumentException("비밀번호에 생년월일이 포함될 수 없습니다.");
        }
        
        // yyyyMMdd 형식
        String yyyyMMdd = birthDateValue.replace("-", "");
        if (password.contains(yyyyMMdd)) {
            throw new IllegalArgumentException("비밀번호에 생년월일이 포함될 수 없습니다.");
        }
        
        // yyMMdd 형식
        String yyMMdd = yyyyMMdd.substring(2);
        if (password.contains(yyMMdd)) {
            throw new IllegalArgumentException("비밀번호에 생년월일이 포함될 수 없습니다.");
        }
    }

    public String encrypt() {
        return passwordEncoder.encode(value);
    }

    public static boolean matches(String rawPassword, String encryptedPassword) {
        if (rawPassword == null || encryptedPassword == null) {
            return false;
        }
        return passwordEncoder.matches(rawPassword, encryptedPassword);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Password password = (Password) o;
        return Objects.equals(value, password.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
