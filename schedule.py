
An = 100
Aq = 10
At = 1
J = An//Aq
K = An%Aq
assert An == J*Aq+K
before_cost = At*(J+1)*(Aq*J/2+K)
def get_reduce_time(m):
    Jl = An//(Aq+m)
    Kl = An%(Aq+m)
    after_cost = At*(Jl+1)*((Aq+m)*Jl/2+Kl)
    reduce_time = before_cost - after_cost
    print(m, after_cost, reduce_time)
    return

for m in range(-Aq+1, 0):
    reduce_time = get_reduce_time(m)
    # print(m,reduce_time)
print('------------')
for m in range(1,An-Aq):
    reduce_time = get_reduce_time(m)
    # print(m,reduce_time)
