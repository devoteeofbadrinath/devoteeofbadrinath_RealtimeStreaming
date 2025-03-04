def count_up_to(n):
    count = 1
    while count <= n:
        yield count
        count += 1  # Function execution pauses here

# Using the generator
gen = count_up_to(5)

print(gen)
print(type(gen))

#for num in gen:
#    print(num)