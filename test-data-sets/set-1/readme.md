## Dataset 0

The files in this folder form a simple, but complete data set for testing the graph
store loader

The graph structure is:

```
                 /----- Document d-1          -----\
                /       Summary 1: 06/08/2022       \
               /        Doc-A                        \
    Entity e-1                                         Entity e-2
    Bob Smith  \                                     / Sally Jones
    03/04/1981  \------ Document d-2          ------/  21/11/1986
        |               Summary 2: 07/08/2022
        |               Doc-B
    Document d-3
    Summary 3, 09/08/2022
    Doc-A
        |
        |
    Entity e-3     ------- Document d-4          ------ Entity e-4
    31 Field Drive         Summary 4, 10/08/2022        Samuel Taylor
    EH36 5PB               Doc-A                        31/12/1990
```
