# Тестовое задание для Golang разработчика
Тестовое задание для кандидата на должность Golang разработчика.

## Описание
Есть внешний сервис, который обрабатывает некие абстрактные объекты батчами. Данный сервис может обрабатывать только определенное количество элементов n в заданный временной интервал p. При превышении ограничения, сервис блокирует последующую обработку на долгое время.

Задача заключается в реализации клиента к данному внешнему сервису и реализацию API для взаимодействия с клиентом, который позволит обрабатывать максимально возможное количество объектов без блокировки. Приводить реализацию внешнего сервиса необязательно!

Определение сервиса можно выразить как.

```golang
package main

import (
	"context"
	"errors"
	"time"
)

// ErrBlocked reports if service is blocked.
var ErrBlocked = errors.New("blocked")

// Service defines external service that can process batches of items.
type Service interface {
	GetLimits() (n uint64, p time.Duration)
	Process(ctx context.Context, batch Batch) error
}

// Batch is a batch of items.
type Batch []Item

// Item is some abstract item.
type Item struct{}
```

## Требования
решение должно быть в git-репозитории (можно прислать архив или опубликовать на github, gitlab, bitbucket...).

## Пожелания
документирование кода;
тесты;
использование статического анализатора (конфигурацию положить в репозиторий).