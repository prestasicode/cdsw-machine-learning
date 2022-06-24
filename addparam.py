'var1'=sys.argv[1]
'var2'=sys.argv[2]

spark.sql("var1 = '{b}' and var2 = '{a}' ")
""".format(a=var2, b=var1)
