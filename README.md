https://github.com/rhusiev-student/s6_apz_lab3/tree/micro_hazelcast

# Запуск сервісів

![service launch](img/1.png)

# Запис 10 повідомлень

Вони розподілені між трьома logging екземплярами:

![msg write](img/2.png)

# Читання повідомлень

![msg read](img/3.png)

# Два екземпляри logging "впали"

Разом з ними автоматично закриваються hazelcast ноди

![service down](img/4.png)

# Читання, коли перші два logging "впали"

Спершу пробує записати в якийсь із тих, що "впали". Потім з другої спроби дістається до працюючого екземпляру:

![msg read 2](img/5.png)

# Увесь цей час існує собі config сервіс

**Коли з'являються** logging чи messages сервіси, то вони **динамічно** кажуть йому свою адресу та свій номер

Потім facade питає в config, які існують адреси - він видає список зареєстрованих екземплярів

![dynamic config](img/dynamic_config.png)
