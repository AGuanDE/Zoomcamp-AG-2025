{# 
    this macro returns the description of the payment_type
#}
{%- macro get_payment_type_description(payment_type) -%}

    case
        cast({{ payment_type }} as integer)
        when 1
        then 'credit card'
        when 2
        then 'cash'
        when 3
        then 'no charge'
        when 4
        then 'dispute'
        when 5
        then 'unknown'
        when 6
        then 'voided trip'
        else 'EMPTY'
    end

{%- endmacro -%}
