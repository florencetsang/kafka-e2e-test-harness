Feature: Value flows through App1 (x multiplier) and App2 (+ constant)

  Scenario Outline: value is transformed to value*2 + 100
    Given a message with id "<id>" and value <input> is published to topic "a"
    When the pipeline processes it within 15 seconds
    Then topic "c" should contain a message with id "<id>" and value <expected>

    Examples:
      | id    | input | expected |
      | t-001 | 5     | 110      |
      | t-002 | 0     | 100      |
      | t-003 | 50    | 200      |
