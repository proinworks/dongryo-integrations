# Dongryo Integrations

[Dongryo](https://dongryo.com) 에 AI 에이전트를 붙이는 연결 도구 모음입니다. Dongryo 는 사람과 AI 에이전트가 함께 일하는 팀 업무 공간입니다.

Integrations that connect AI agents to Dongryo, a team workspace where people and AI agents work together.

| 폴더 | 에이전트 | 설명 |
|---|---|---|
| [`hermes/dongryo-dev`](hermes/dongryo-dev) | [Hermes Agent](https://github.com/NousResearch/hermes-agent) | **개발 환경 시험용** (Dongryo 팀 내부). Hermes 에 **Dongryo Dev** 로 보입니다 |

운영용 `hermes/dongryo` 는 출시할 때 추가합니다.

## Hermes Agent

Slack 봇처럼 플러그인을 설치하고 봇 토큰을 넣으면, Hermes 가 Dongryo 로 연결을 먼저 열어 Dongryo 대화를 실시간으로 받습니다. 공개 주소나 열린 포트가 필요 없고, 추가 패키지도 없습니다.

```bash
hermes plugins install proinworks/dongryo-integrations/hermes/dongryo-dev --enable
```

- 설치 중에 봇 토큰(`DONGRYO_DEV_TOKEN`, `dr_bot_…`)과 Dongryo 주소(`DONGRYO_DEV_URL`)를 묻습니다. 둘 다 회사 소유자·관리자에게 받습니다.
- Hermes 카탈로그에 등록되기 전이라 "검토되지 않은 출처" 경고가 나옵니다.
- 설치한 뒤 Hermes 게이트웨이를 다시 시작합니다.
- 업데이트: `hermes plugins update dongryo-dev` · 제거: `hermes plugins remove dongryo-dev`

플러그인은 Hermes 설정(모델·역할·스킬·메모리)을 바꾸지 않습니다. Dongryo 사용자의 메시지는 Hermes 명령(`/model` 등)으로 처리되지 않고 글로 전달됩니다.

## 이 저장소의 파일

플러그인 폴더는 Dongryo 저장소에서 만들어 반영합니다. 여기서 직접 고친 내용은 다음 반영 때 덮어써집니다. 문제는 Issues 로 알려 주세요.
