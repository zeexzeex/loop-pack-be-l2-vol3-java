package com.loopers.application.user;

import com.loopers.domain.user.*;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class UserFacade {

    private final UserService userService;

    public UserFacade(UserService userService) {
        this.userService = userService;
    }

    public UserInfo signUp(
        String userId,
        String password,
        String email,
        String birthDate,
        String genderValue
    ) {
        UserId userIdVO = new UserId(userId);
        Email emailVO = new Email(email);
        BirthDate birthDateVO = new BirthDate(birthDate);
        Password passwordVO = Password.of(password, birthDateVO);
        Gender gender = Gender.from(genderValue);

        UserModel user = userService.signUp(userIdVO, emailVO, birthDateVO, passwordVO, gender);
        return UserInfo.from(user);
    }

    public UserInfo getMyInfo(String userId) {
        return userService.getMyInfo(userId)
            .map(UserInfo::from)
            .orElse(null);
    }

    public PointsInfo getPoints(String userId) {
        Long points = userService.getPoints(userId);
        if (points == null) {
            return null;
        }
        return new PointsInfo(userId, points);
    }

    public void updatePassword(String userId, String currentPassword, String newPassword) {
        userService.updatePassword(userId, currentPassword, newPassword);
    }

    /**
     * 로그인 ID로 고객의 PK를 조회한다.
     * Like·Cart·Order 등 도메인에서 사용할 userId(Long) 변환용.
     */
    public Optional<Long> findUserIdByLoginId(String loginId) {
        return userService.getMyInfo(loginId).map(UserModel::getId);
    }
}
