package com.loopers.domain.user;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PasswordTest {

    @DisplayName("비밀번호를 생성할 때, ")
    @Nested
    class Create {

        @DisplayName("8자 미만이면, IllegalArgumentException이 발생한다.")
        @Test
        void create_withLessThan8Characters_shouldFail() {
            // given
            String shortPassword = "Pass12!";
            BirthDate birthDate = new BirthDate("1990-01-15");

            // when & then
            assertThrows(IllegalArgumentException.class, () -> {
                Password.of(shortPassword, birthDate);
            });
        }

        @DisplayName("16자를 초과하면, IllegalArgumentException이 발생한다.")
        @Test
        void create_withMoreThan16Characters_shouldFail() {
            // given
            String longPassword = "Pass1234567890123!";
            BirthDate birthDate = new BirthDate("1990-01-15");

            // when & then
            assertThrows(IllegalArgumentException.class, () -> {
                Password.of(longPassword, birthDate);
            });
        }

        @DisplayName("생년월일(yyyyMMdd)이 포함되면, IllegalArgumentException이 발생한다.")
        @Test
        void create_withBirthDateYyyyMMdd_shouldFail() {
            // given
            String password = "Pass19900115!";
            BirthDate birthDate = new BirthDate("1990-01-15");

            // when & then
            assertThrows(IllegalArgumentException.class, () -> {
                Password.of(password, birthDate);
            });
        }

        @DisplayName("생년월일(yyMMdd)이 포함되면, IllegalArgumentException이 발생한다.")
        @Test
        void create_withBirthDateYyMMdd_shouldFail() {
            // given
            String password = "Pass900115!";
            BirthDate birthDate = new BirthDate("1990-01-15");

            // when & then
            assertThrows(IllegalArgumentException.class, () -> {
                Password.of(password, birthDate);
            });
        }

        @DisplayName("생년월일(yyyy-MM-dd)이 포함되면, IllegalArgumentException이 발생한다.")
        @Test
        void create_withBirthDateWithHyphen_shouldFail() {
            // given
            String password = "Pass1990-01-15!";
            BirthDate birthDate = new BirthDate("1990-01-15");

            // when & then
            assertThrows(IllegalArgumentException.class, () -> {
                Password.of(password, birthDate);
            });
        }

        @DisplayName("8-16자이고 생년월일이 포함되지 않으면, 정상적으로 생성된다.")
        @Test
        void create_withValidPasswordWithoutBirthDate_shouldSuccess() {
            // given
            String validPassword = "SecurePass1!";
            BirthDate birthDate = new BirthDate("1990-01-15");

            // when
            Password password = Password.of(validPassword, birthDate);

            // then
            assertThat(password).isNotNull();
        }

        @DisplayName("null이면, IllegalArgumentException이 발생한다.")
        @Test
        void create_withNull_shouldFail() {
            // given
            BirthDate birthDate = new BirthDate("1990-01-15");

            // when & then
            assertThrows(IllegalArgumentException.class, () -> {
                Password.of(null, birthDate);
            });
        }

        @DisplayName("빈 문자열이면, IllegalArgumentException이 발생한다.")
        @Test
        void create_withBlank_shouldFail() {
            // given
            BirthDate birthDate = new BirthDate("1990-01-15");

            // when & then
            assertThrows(IllegalArgumentException.class, () -> {
                Password.of("  ", birthDate);
            });
        }

        @DisplayName("생년월일이 null이면, IllegalArgumentException이 발생한다.")
        @Test
        void create_withNullBirthDate_shouldFail() {
            // given
            String password = "SecurePass1!";

            // when & then
            assertThrows(IllegalArgumentException.class, () -> {
                Password.of(password, null);
            });
        }

        @DisplayName("정확히 8자이면, 정상적으로 생성된다.")
        @Test
        void create_withExactly8Characters_shouldSuccess() {
            // given
            String password = "Pass12!a";
            BirthDate birthDate = new BirthDate("1990-01-15");

            // when
            Password result = Password.of(password, birthDate);

            // then
            assertThat(result).isNotNull();
        }

        @DisplayName("정확히 16자이면, 정상적으로 생성된다.")
        @Test
        void create_withExactly16Characters_shouldSuccess() {
            // given
            String password = "Pass12!abcdefgh1";
            BirthDate birthDate = new BirthDate("1990-01-15");

            // when
            Password result = Password.of(password, birthDate);

            // then
            assertThat(result).isNotNull();
        }
    }

    @DisplayName("비밀번호를 암호화할 때, ")
    @Nested
    class Encrypt {

        @DisplayName("암호화된 값이 원본과 달라야 한다.")
        @Test
        void encrypt_shouldReturnDifferentValue() {
            // given
            String rawPassword = "SecurePass1!";
            BirthDate birthDate = new BirthDate("1990-01-15");
            Password password = Password.of(rawPassword, birthDate);

            // when
            String encrypted = password.encrypt();

            // then
            assertThat(encrypted).isNotEqualTo(rawPassword);
            assertThat(encrypted).isNotBlank();
        }
    }

    @DisplayName("비밀번호를 검증할 때, ")
    @Nested
    class Matches {

        @DisplayName("원본 비밀번호와 암호화된 비밀번호가 일치하면, true를 반환한다.")
        @Test
        void matches_withCorrectPassword_shouldReturnTrue() {
            // given
            String rawPassword = "SecurePass1!";
            BirthDate birthDate = new BirthDate("1990-01-15");
            Password password = Password.of(rawPassword, birthDate);
            String encryptedPassword = password.encrypt();

            // when
            boolean matches = Password.matches(rawPassword, encryptedPassword);

            // then
            assertThat(matches).isTrue();
        }

        @DisplayName("원본 비밀번호와 암호화된 비밀번호가 일치하지 않으면, false를 반환한다.")
        @Test
        void matches_withIncorrectPassword_shouldReturnFalse() {
            // given
            String correctPassword = "SecurePass1!";
            String wrongPassword = "WrongPass2!";
            BirthDate birthDate = new BirthDate("1990-01-15");
            Password password = Password.of(correctPassword, birthDate);
            String encryptedPassword = password.encrypt();

            // when
            boolean matches = Password.matches(wrongPassword, encryptedPassword);

            // then
            assertThat(matches).isFalse();
        }

        @DisplayName("rawPassword가 null이면, false를 반환한다.")
        @Test
        void matches_withNullRawPassword_shouldReturnFalse() {
            // given
            String rawPassword = "SecurePass1!";
            BirthDate birthDate = new BirthDate("1990-01-15");
            Password password = Password.of(rawPassword, birthDate);
            String encryptedPassword = password.encrypt();

            // when
            boolean result = Password.matches(null, encryptedPassword);

            // then
            assertThat(result).isFalse();
        }

        @DisplayName("encryptedPassword가 null이면, false를 반환한다.")
        @Test
        void matches_withNullEncryptedPassword_shouldReturnFalse() {
            // when
            boolean result = Password.matches("SecurePass1!", null);

            // then
            assertThat(result).isFalse();
        }
    }

    @DisplayName("값 객체 동등성")
    @Nested
    class EqualsAndHashCode {

        @DisplayName("동일한 raw 비밀번호로 생성한 두 인스턴스는 equals가 true이다.")
        @Test
        void equals_withSameValue_shouldReturnTrue() {
            BirthDate birthDate = new BirthDate("1990-01-15");
            Password a = Password.of("SecurePass1!", birthDate);
            Password b = Password.of("SecurePass1!", birthDate);

            assertThat(a).isEqualTo(b);
            assertThat(a.hashCode()).isEqualTo(b.hashCode());
        }

        @DisplayName("다른 raw 비밀번호로 생성한 두 인스턴스는 equals가 false이다.")
        @Test
        void equals_withDifferentValue_shouldReturnFalse() {
            BirthDate birthDate = new BirthDate("1990-01-15");
            Password a = Password.of("SecurePass1!", birthDate);
            Password b = Password.of("OtherPass12!", birthDate);

            assertThat(a).isNotEqualTo(b);
            assertThat(a.hashCode()).isNotEqualTo(b.hashCode());
        }

        @DisplayName("null 또는 다른 타입과는 equals가 false이다.")
        @Test
        void equals_withNullOrOtherType_shouldReturnFalse() {
            BirthDate birthDate = new BirthDate("1990-01-15");
            Password p = Password.of("SecurePass1!", birthDate);

            assertThat(p).isNotEqualTo(null);
            assertThat(p).isNotEqualTo("SecurePass1!");
        }
    }
}
