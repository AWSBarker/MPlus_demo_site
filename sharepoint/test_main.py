
def drop_plot(state='testing'): # if state not 'testing' AND filter > 0, send email otherwise drop (always make plot for demo)

    if state != 'testing':
        print(f'NOT testing. Main is {__name__}')
    else:
        print('testing')

if __name__ == "__main__":
    drop_plot('production')
