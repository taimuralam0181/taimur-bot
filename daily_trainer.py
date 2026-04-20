from self_learning import build_training_report, train_model


def main() -> None:
    model = train_model()
    print(build_training_report(model))


if __name__ == "__main__":
    main()
